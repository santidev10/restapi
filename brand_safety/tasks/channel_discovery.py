from brand_safety.tasks.channel_update_helper import channel_update_helper
from brand_safety.tasks.constants import Schedulers
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder
from saas.configs.celery import Queue
from saas.configs.celery import TaskExpiration
from saas import celery_app
from utils.celery.tasks import celery_lock


@celery_app.task(bind=True)
@celery_lock(Schedulers.ChannelDiscovery.NAME, expire=TaskExpiration.BRAND_SAFETY_CHANNEL_DISCOVERY, max_retries=0)
def channel_discovery_scheduler():
    channel_manager = ChannelManager()
    query = channel_manager.forced_filters() \
            & QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get()
    channel_update_helper(query, Queue.BRAND_SAFETY_CHANNEL_PRIORITY)
