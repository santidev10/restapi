from brand_safety.tasks.channel_update_helper import channel_update_helper
from brand_safety.tasks.constants import Schedulers
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder
from saas import celery_app
from saas.configs.celery import Queue
from saas.configs.celery import TaskExpiration
from utils.celery.tasks import celery_lock
from utils.celery.utils import get_queue_size


@celery_app.task(bind=True)
@celery_lock(Schedulers.ChannelOutdated.NAME, expire=TaskExpiration.BRAND_SAFETY_CHANNEL_OUTDATED, max_retries=0)
def channel_outdated_scheduler():
    """
    Celery task to discover Channels with outdated brand safety data and add to queue

    In order to keep queue from getting too large or from channels being scored repeatedly, the queue size is
        checked to be below a certain size before adding items to the queue.
        Since the scheduler runs frequently, it may add items to the queue before the workers consuming from this
        queue have processed them, leading to inefficient, multiple processing of the same items.
    :return:
    """
    if get_queue_size(Queue.BRAND_SAFETY_CHANNEL_LIGHT) <= Schedulers.ChannelOutdated.get_minimum_threshold():
        channel_manager = ChannelManager()
        query = channel_manager.forced_filters() \
                & QueryBuilder().build().must().range().field("brand_safety.updated_at") \
                    .lte(Schedulers.ChannelOutdated.UPDATE_TIME_THRESHOLD).get()
        sorts = ("brand_safety.updated_at", "-stats.subscribers")
        channel_update_helper(Schedulers.ChannelOutdated, query, Queue.BRAND_SAFETY_CHANNEL_LIGHT, sort=sorts)
