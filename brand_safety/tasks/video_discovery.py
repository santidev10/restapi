from celery import group

from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.tasks.constants import Schedulers
from es_components.query_builder import QueryBuilder
from es_components.constants import Sections
from es_components.managers import VideoManager
from saas import celery_app
from saas.configs.celery import Queue
from saas.configs.celery import TaskExpiration
from utils.celery.tasks import celery_lock
from utils.celery.utils import get_queue_size


@celery_app.task(bind=True)
@celery_lock(Schedulers.VideoDiscovery.NAME, max_retries=0, expire=TaskExpiration.BRAND_SAFETY_VIDEO_DISCOVERY, max_retries=0)
def video_discovery_scheduler():
    video_manager = VideoManager(upsert_sections=(Sections.BRAND_SAFETY,))
    query = video_manager.forced_filters() \
            & QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get()
    queue_size = get_queue_size(Queue.BRAND_SAFETY_VIDEO_PRIORITY)
    limit = Schedulers.VideoDiscovery.MAX_QUEUE_SIZE - queue_size

    task_signatures = []
    for _ in range(limit):
        videos = video_manager.search(query, limit=Schedulers.VideoDiscovery.TASK_BATCH_SIZE).execute()
        ids = [item.main.id for item in videos]
        task_signatures.append(video_update.si(ids).set(queue=Queue.BRAND_SAFETY_VIDEO_PRIORITY))
        # Create brand_safety section so next discovery batch does not overlap
        video_manager.upsert(videos)
    group(task_signatures).apply_async()


@celery_app.task
def video_update(video_ids):
    auditor = BrandSafetyAudit()
    auditor.process_videos(video_ids)

