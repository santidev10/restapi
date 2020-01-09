from celery import chain

from audit_tool.models import APIScriptTracker
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.tasks.constants import Schedulers
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder
from saas import celery_app
from saas.configs.celery import Queue
from saas.configs.celery import TaskExpiration
from utils.celery.tasks import celery_lock
from utils.celery.tasks import group_chorded
from utils.celery.utils import get_queue_size
from utils.utils import chunks_generator


@celery_app.task(bind=True)
@celery_lock(Schedulers.ChannelUpdate.NAME, expire=TaskExpiration.BRAND_SAFETY_CHANNEL_UPDATE, max_retries=0)
def channel_update_scheduler():
    cursor, _ = APIScriptTracker.objects.get_or_create(
        name=Schedulers.ChannelUpdate.NAME,
        defaults={
            "cursor_id": None
        }
    )
    channel_manager = ChannelManager(upsert_sections=(Sections.BRAND_SAFETY,))
    query = channel_manager.forced_filters() \
            & QueryBuilder().build().must().range().field(MAIN_ID_FIELD).gte(cursor.cursor_id).get() \
            & QueryBuilder().build().must().range().field("brand_safety.updated_at").lte(Schedulers.ChannelUpdate.UPDATE_TIME_THRESHOLD).get()
    queue_size = get_queue_size(Queue.BRAND_SAFETY_CHANNEL_LIGHT)
    limit = Schedulers.ChannelUpdate.get_items_limit(queue_size)
    channels = channel_manager.search(query, limit=min(limit, 10000), sort=(MAIN_ID_FIELD, f"{Sections.STATS}.subscribers")).execute()
    channel_ids = [item.main.id for item in channels]
    args = [list(batch) for batch in chunks_generator(channel_ids, size=Schedulers.ChannelUpdate.TASK_BATCH_SIZE)]
    if args:
        for arg in args:
            try:
                last_id = arg[-1]
            except IndexError:
                last_id = None
            # Update brand_safety section so next discovery batch does not overlap
            channel_manager.upsert(channels)
            chain(
                channel_update.si(arg),
                finalize.si(last_id),
            ).apply_async(queue=Queue.BRAND_SAFETY_CHANNEL_LIGHT)
    else:
        finalize.delay(None)


@celery_app.task
def channel_update(channel_ids):
    if channel_ids is not None:
        if type(channel_ids) is str:
            channel_ids = [channel_ids]
        auditor = BrandSafetyAudit()
        auditor.process_channels(channel_ids)


@celery_app.task
def finalize(last_id):
    cursor = APIScriptTracker.objects.get(name=Schedulers.ChannelUpdate.NAME)
    curr_cursor = cursor.cursor_id if cursor.cursor_id is not None else ""
    if last_id is not None and last_id > curr_cursor:
        cursor.cursor_id = last_id
    else:
        cursor.cursor_id = None
    cursor.save()
