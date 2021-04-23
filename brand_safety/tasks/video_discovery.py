from celery import group

from brand_safety.auditors.video_auditor import VideoAuditor
from brand_safety.tasks.constants import Schedulers
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from saas import celery_app
from saas.configs.celery import Queue
from saas.configs.celery import TaskExpiration
from utils.celery.tasks import celery_lock
from utils.celery.utils import get_queue_size
from utils.utils import chunks_generator


@celery_app.task(bind=True)
@celery_lock(Schedulers.VideoDiscovery.NAME, expire=TaskExpiration.BRAND_SAFETY_VIDEO_DISCOVERY, max_retries=0)
def video_discovery_scheduler():
    """
    Discovers Videos that have either rescore=True or have never been scored before
    rescore field is used when videos need to be updated immediately and are prioritized first when scheduler runs.

    In order to keep queue from getting too large or from videos being scored repeatedly, the queue size is
        checked to be below a certain size before adding items to the queue.
        Since the scheduler runs frequently, it may add items to the queue before the workers consuming from this
        queue have processed them, leading to inefficient, multiple processing of the same items.
    """
    video_manager = VideoManager(upsert_sections=(Sections.BRAND_SAFETY,))
    queue_size = get_queue_size(Queue.BRAND_SAFETY_VIDEO_PRIORITY)

    if queue_size <= Schedulers.VideoDiscovery.get_minimum_threshold():
        base_query = video_manager.forced_filters()
        _create_rescore_tasks(video_manager, base_query)

        batch_limit = (Schedulers.VideoDiscovery.MAX_QUEUE_SIZE - queue_size)
        batch_count = 0
        with_no_score = base_query & QueryBuilder().build().must_not().exists().field(f"{Sections.BRAND_SAFETY}.overall_score").get()
        task_signatures = []
        search = video_manager.search(with_no_score, limit=5000).source([MAIN_ID_FIELD])
        for chunk in chunks_generator(search.execute(), size=Schedulers.VideoDiscovery.TASK_BATCH_SIZE):
            ids = [video.main.id for video in chunk]
            task_signatures.append(video_update.si(ids).set(queue=Queue.BRAND_SAFETY_VIDEO_PRIORITY))
            batch_count += 1
            if batch_count >= batch_limit or batch_count >= Schedulers.VideoDiscovery.MAX_QUEUE_SIZE:
                break
        group(task_signatures).apply_async()


@celery_app.task
def video_update(video_ids, rescore=False):
    if isinstance(video_ids, str):
        video_ids = [video_ids]
    auditor = VideoAuditor()
    auditor.process(video_ids, as_rescore=rescore)
    to_rescore = auditor.channels_to_rescore
    # Add rescore flag for channels that should be rescored by channel discovery task as newly scored videos under this
    # channel may have scored low, and may largely affect how a channel would score
    rescore_channels = auditor.channel_manager.get(to_rescore, skip_none=True)
    for channel in rescore_channels:
        channel.brand_safety.rescore = True
    auditor.channel_manager.upsert(rescore_channels, ignore_update_time_sections=[Sections.BRAND_SAFETY])


def _create_rescore_tasks(video_manager, base_query):
    """
    Create group of tasks to prioritize rescoring videos
    :param base_query: forced filters query shared by rescore tasks and general discovery in video_discovery_scheduler
    """
    task_signatures = []
    rescore_query = base_query & QueryBuilder().build().must().term().field(f"{Sections.BRAND_SAFETY}.rescore").value(True).get()
    rescore_ids = [v.main.id for v in video_manager.search(rescore_query).execute()]
    for chunk in chunks_generator(rescore_ids, size=Schedulers.VideoDiscovery.TASK_BATCH_SIZE):
        task_signatures.append(video_update.si(list(chunk), rescore=True).set(queue=Queue.BRAND_SAFETY_VIDEO_PRIORITY))
    group(task_signatures).apply_async()
