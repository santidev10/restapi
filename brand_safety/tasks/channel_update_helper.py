from celery import group

from brand_safety.tasks.channel_update import channel_update
from es_components.constants import Sections
from es_components.managers import ChannelManager
from utils.celery.utils import get_queue_size
from utils.utils import chunks_generator


def channel_update_helper(scheduler, query, queue, sort=("-stats.subscribers",)):
    channel_manager = ChannelManager(upsert_sections=(Sections.BRAND_SAFETY,))
    queue_size = get_queue_size(queue)
    limit = scheduler.get_items_limit(queue_size)
    channels = channel_manager.search(query, limit=min(limit, 10000), sort=sort).execute()
    channel_ids = [item.main.id for item in channels]
    args = [list(batch) for batch in chunks_generator(channel_ids, size=scheduler.TASK_BATCH_SIZE)]
    # Create /update brand_safety section so next discovery/update batch does not overlap
    channel_manager.upsert(channels)
    group([
        channel_update.si(arg).set(queue=queue)
        for arg in args
    ]).apply_async()
