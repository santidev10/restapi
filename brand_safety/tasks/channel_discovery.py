from brand_safety.tasks.channel_update_helper import channel_update_helper
from brand_safety.tasks.constants import Schedulers
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder
from saas import celery_app
from saas.configs.celery import Queue
from saas.configs.celery import TaskExpiration
from utils.celery.tasks import celery_lock
from utils.celery.utils import get_queue_size


@celery_app.task(bind=True)
@celery_lock(Schedulers.ChannelDiscovery.NAME, expire=TaskExpiration.BRAND_SAFETY_CHANNEL_DISCOVERY, max_retries=0)
def channel_discovery_scheduler():
    """
    Celery task to discover Channels with no brand safety data and add tasks to queue to be scored
    Queue channels with rescore = True or have no brand safety overall score

    In order to keep queue from getting too large or from channels being scored repeatedly, the queue size is
        checked to be below a certain size before adding items to the queue.
        Since the scheduler runs frequently, it may add items to the queue before the workers consuming from this
        queue have processed them, leading to inefficient, multiple processing of the same items.
    """
    if get_queue_size(Queue.BRAND_SAFETY_CHANNEL_PRIORITY) <= Schedulers.ChannelDiscovery.get_minimum_threshold():
        channel_manager = ChannelManager()
        base_query = channel_manager.forced_filters()

        query_with_rescore = base_query & QueryBuilder().build().must().term().field(f"{Sections.BRAND_SAFETY}.rescore").value(True).get()
        channel_update_helper(
            Schedulers.ChannelDiscovery, query_with_rescore, Queue.BRAND_SAFETY_CHANNEL_PRIORITY,
            sort=("-stats.subscribers",), rescore=True
        )

        query_with_no_score = base_query & QueryBuilder().build().must_not().exists().field(f"{Sections.BRAND_SAFETY}.overall_score").get()
        channel_update_helper(
            Schedulers.ChannelDiscovery, query_with_no_score, Queue.BRAND_SAFETY_CHANNEL_PRIORITY,
            sort=("-stats.subscribers",)
        )
