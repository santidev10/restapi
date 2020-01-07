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
    channel_manager = ChannelManager()
    query = channel_manager.forced_filters() \
            & QueryBuilder().build().must().range().field(MAIN_ID_FIELD).gte(cursor.cursor_id).get() \
            & QueryBuilder().build().must().range().field("brand_safety.updated_at").lte(Schedulers.ChannelUpdate.UPDATE_TIME_THRESHOLD).get()
    limit = Schedulers.ChannelUpdate.TASK_BATCH_SIZE * Schedulers.ChannelUpdate.MAX_QUEUE_SIZE
    channels = channel_manager.search(query, limit=min(limit, 10000), sort=(MAIN_ID_FIELD, f"{Sections.STATS}.subscribers")).execute()
    channel_ids = [item.main.id for item in channels]
    try:
        last_id = channel_ids[-1]
    except IndexError:
        last_id = None
    args = [list(batch) for batch in chunks_generator(channel_ids, size=Schedulers.ChannelUpdate.TASK_BATCH_SIZE)]
    update_tasks = group_chorded([
        channel_update.si(arg).set(queue=Queue.BRAND_SAFETY_CHANNEL_LIGHT) for arg in args
    ])
    task = chain(
        update_tasks,
        finalize.si(last_id).set(queue=Queue.SCHEDULERS),
    )
    return task()


@celery_app.task
def channel_update(channel_ids):
    if type(channel_ids) is str:
        channel_ids = [channel_ids]
    auditor = BrandSafetyAudit()
    auditor.process_channels(channel_ids)


@celery_app.task
def finalize(last_id):
    cursor = APIScriptTracker.objects.get(name=Schedulers.ChannelUpdate.NAME)
    cursor.cursor_id = last_id
    cursor.save()
