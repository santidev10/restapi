from celery import group

from elasticsearch_dsl import Q

from brand_safety.tasks.channel_update import channel_update
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models import Channel
from utils.celery.utils import get_queue_size
from utils.utils import chunks_generator


def channel_update_helper(scheduler, query: Q, queue: str, sort=("-stats.subscribers",), rescore=False):
    """
    Helper function to accept parameters and add items to the target celery queue
    :param scheduler: Schedulers class configuration
    :param query: Elasticsearch query used to retrieve items to add to queue
    :param queue: Name of target queue to add
    :param sort:
    :param rescore: Boolean to determine if current query is retrieving rescore=True items. If True,
        then this will update items processed to be rescore=False
    """
    channel_manager = ChannelManager(sections=(Sections.BRAND_SAFETY,))
    queue_size = get_queue_size(queue)
    limit = scheduler.get_items_limit(queue_size)
    channels = channel_manager.search(query, limit=min(limit, 10000), sort=sort).execute()
    channel_ids = [item.main.id for item in channels]
    args = [list(batch) for batch in chunks_generator(channel_ids, size=scheduler.TASK_BATCH_SIZE)]
    group([
        channel_update.si(arg).set(queue=queue)
        for arg in args
    ]).apply_async()

    if rescore is True:
        # Update channels rescore values that are rescored
        update_rescore_channels = channel_manager.get(channel_ids, skip_none=True)
        for channel in channels:
            channel.brand_safety.rescore = False
        channel_manager.upsert(update_rescore_channels, ignore_update_time_sections=[Sections.BRAND_SAFETY])
