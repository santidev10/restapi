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
    video_manager = VideoManager(upsert_sections=(Sections.BRAND_SAFETY,))
    queue_size = get_queue_size(Queue.BRAND_SAFETY_VIDEO_PRIORITY)

    if queue_size <= Schedulers.VideoDiscovery.get_minimum_threshold():
        base_query = video_manager.forced_filters()
        task_signatures = []

        rescore_ids = get_rescore_ids(video_manager, base_query)
        task_signatures.append(video_update.si(rescore_ids, rescore=True).set(queue=Queue.BRAND_SAFETY_VIDEO_PRIORITY))

        batch_limit = (Schedulers.VideoDiscovery.MAX_QUEUE_SIZE - queue_size) * Schedulers.VideoDiscovery.TASK_BATCH_SIZE
        with_no_score = base_query & QueryBuilder().build().must_not().exists().field(f"{Sections.BRAND_SAFETY}.overall_score").get()
        no_score_ids = video_manager.search(with_no_score, limit=min(10000, batch_limit)).execute()
        for batch in chunks_generator(no_score_ids, Schedulers.VideoDiscovery.TASK_BATCH_SIZE):
            ids = [video.main.id for video in batch]
            task_signatures.append(video_update.si(ids).set(queue=Queue.BRAND_SAFETY_VIDEO_PRIORITY))
        group(task_signatures).apply_async()


@celery_app.task
def video_update(video_ids, rescore=False):
    if isinstance(video_ids, str):
        video_ids = [video_ids]
    auditor = VideoAuditor()
    auditor.process(video_ids)
    to_rescore = auditor.channels_to_rescore
    # Add rescore flag to be rescored by channel discovery task
    query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value(to_rescore).get()
    auditor.channel_manager.update_rescore(query, rescore=True, conflicts="proceed")

    # Update video rescore batch to False
    if rescore is True:
        manager = VideoManager(upsert_sections=(Sections.BRAND_SAFETY,))
        manager.update_rescore(manager.ids_query(video_ids), rescore=False, conflicts="proceed")


def get_rescore_ids(manager, base_query):
    with_rescore = base_query & QueryBuilder().build().must().term().field(f"{Sections.BRAND_SAFETY}.rescore").value(True).get()
    ids = [v.main.id for v in manager.search(with_rescore).execute()]
    return ids