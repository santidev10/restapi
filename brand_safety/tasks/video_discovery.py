from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.tasks.constants import Schedulers
from es_components.query_builder import QueryBuilder
from es_components.constants import Sections
from es_components.managers import VideoManager
from saas import celery_app
from saas.configs.celery import Queue
from utils.celery.tasks import celery_lock
from utils.celery.utils import get_queue_size
from utils.utils import chunks_generator


@celery_app.task
@celery_lock(Schedulers.VideoDiscovery.NAME)
def video_discovery_scheduler():
    video_manager = VideoManager()
    query = video_manager.forced_filters() \
            & QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get()
    queue_size = get_queue_size(Queue.BRAND_SAFETY_VIDEO_PRIORITY)
    limit = Schedulers.VideoDiscovery.MAX_QUEUE_SIZE - queue_size
    if limit > 0:
        videos = video_manager.search(query, limit=limit).execute()
        video_ids = [item.main.id for item in videos]
        for batch in chunks_generator(video_ids, size=Schedulers.VideoDiscovery.TASK_BATCH_SIZE):
            video_update.delay(list(batch))


@celery_app.task
def video_update(video_ids):
    auditor = BrandSafetyAudit()
    auditor.process_videos(video_ids)

