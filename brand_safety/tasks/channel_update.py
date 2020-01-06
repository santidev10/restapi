from celery import chain

from audit_tool.models import APIScriptTracker
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.tasks.constants import Schedulers
from es_components.constants import MAIN_ID_FIELD
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder
from saas import celery_app
from saas.configs.celery import Queue
from utils.celery.tasks import celery_lock
from utils.celery.tasks import group_chorded
from utils.celery.utils import get_queue_size
from utils.utils import chunks_generator


@celery_app.task
@celery_lock(Schedulers.ChannelUpdate.NAME)
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

    queue_size = get_queue_size(Queue.BRAND_SAFETY_CHANNEL_LIGHT)
    limit = Schedulers.ChannelUpdate.MAX_QUEUE_SIZE - queue_size
    if limit > 0 or True:
        limit = 10
        channels = channel_manager.search(query, limit=limit, sort=("-stats.subscribers",)).execute()
        channel_ids = [item.main.id for item in channels]
        try:
            last_id = channel_ids[-1]
        except IndexError:
            last_id = None
        args = [list(batch) for batch in chunks_generator(channel_ids, size=Schedulers.ChannelUpdate.TASK_BATCH_SIZE)]
        update_tasks = group_chorded([
            channel_update.si(arg) for arg in args
        ])
        task = chain(
            update_tasks,
            finalize.si(last_id),
        )
        return task()


@celery_app.task
def channel_update(channel_ids):
    auditor = BrandSafetyAudit()
    auditor.process_channels(channel_ids)


@celery_app.task
def finalize(last_id):
    cursor = APIScriptTracker.objects.get(name=Schedulers.ChannelUpdate.NAME)
    cursor.cursor_id = last_id
    cursor.save()
