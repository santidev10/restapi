from brand_safety.tasks.channel_update_helper import channel_update_helper
from brand_safety.tasks.constants import Schedulers
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder
from saas import celery_app
from saas.configs.celery import Queue
from saas.configs.celery import TaskExpiration
from utils.celery.tasks import celery_lock


@celery_app.task(bind=True)
@celery_lock(Schedulers.ChannelDiscovery.NAME, expire=TaskExpiration.BRAND_SAFETY_CHANNEL_DISCOVERY, max_retries=0)
def channel_discovery_scheduler():
    """ Queue channels with rescore = True or have no brand safety overall score """
    channel_manager = ChannelManager()
    query = channel_manager.forced_filters() \
        & QueryBuilder().build().must_not().exists().field(f"{Sections.TASK_US_DATA}").get()
    query.should = [
        QueryBuilder().build().must().term().field(f"{Sections.BRAND_SAFETY}.rescore").value(True).get(),
        QueryBuilder().build().must_not().exists().field(f"{Sections.BRAND_SAFETY}.overall_score").get()
    ]
    channel_update_helper(
        Schedulers.ChannelDiscovery, query, Queue.BRAND_SAFETY_CHANNEL_PRIORITY,
        sort=("-brand_safety.rescore", "-stats.subscribers")
    )
