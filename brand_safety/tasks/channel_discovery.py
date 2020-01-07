from celery import group

from brand_safety.tasks.channel_update import channel_update
from brand_safety.tasks.constants import Schedulers
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder
from saas.configs.celery import Queue
from saas.configs.celery import TaskExpiration
from saas import celery_app
from utils.celery.tasks import celery_lock
from utils.celery.utils import get_queue_size
from utils.utils import chunks_generator


@celery_app.task(bind=True)
@celery_lock(Schedulers.ChannelDiscovery.NAME, expire=TaskExpiration.BRAND_SAFETY_CHANNEL_DISCOVERY, max_retries=0)
def channel_discovery_scheduler():
    channel_manager = ChannelManager(upsert_sections=(Sections.BRAND_SAFETY,))
    query = channel_manager.forced_filters() \
            & QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get()
    queue_size = get_queue_size(Queue.BRAND_SAFETY_CHANNEL_PRIORITY)
    limit = Schedulers.ChannelDiscovery.get_items_limit(queue_size)
    channels = channel_manager.search(query, limit=min(limit, 10000), sort=("stats.subscribers",)).execute()
    channel_ids = [item.main.id for item in channels]

    args = [list(batch) for batch in chunks_generator(channel_ids, size=Schedulers.ChannelDiscovery.TASK_BATCH_SIZE)]
    group([
        channel_update.si(arg).set(queue=Queue.BRAND_SAFETY_CHANNEL_PRIORITY)
        for arg in args
    ]).apply_async()
    # Create brand_safety section so next discovery batch does not overlap
    channel_manager.upsert(channels)
