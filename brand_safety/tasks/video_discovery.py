from audit_tool.models import APIScriptTracker
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.constants import CHANNEL_DISCOVERY_TRACKER
from brand_safety.tasks.audit_manager import AuditManager
from es_components.query_builder import QueryBuilder
from es_components.managers import VideoManager
from es_components.constants import Sections
from es_components.constants import MAIN_ID_FIELD
from utils.celery.utils import get_queue_size
from saas.configs.celery import Queue
from brand_safety.tasks.constants import Schedulers
from brand_safety.tasks.constants import VIDEO_FIELDS
from utils.utils import chunks_generator
from saas import celery_app
from utils.celery.tasks import celery_lock


@celery_app.task
@celery_lock(Schedulers.VideoDiscovery.NAME)
def video_discovery_scheduler():
    video_manager = VideoManager(
        sections=(Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.CHANNEL, Sections.BRAND_SAFETY,
                  Sections.CAPTIONS, Sections.CUSTOM_CAPTIONS),
        upsert_sections=(Sections.BRAND_SAFETY, Sections.CHANNEL)
    )
    query = video_manager.forced_filters() \
            & QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get()
    queue_remains = get_queue_size(Queue.BRAND_SAFETY)
    videos = video_manager.search(query, limit=100).source(VIDEO_FIELDS).execute()
    for batch in chunks_generator(videos, size=Schedulers.VideoDiscovery.TASK_BATCH_SIZE):
        video_discovery.delay(batch)


@celery_app.task
def video_discovery(video_ids):
    auditor = BrandSafetyAudit()
    auditor.process_videos(video_ids)

