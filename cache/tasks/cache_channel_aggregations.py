import logging

from saas import celery_app
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager

from cache.constants import CHANNEL_AGGREGATIONS_KEY
from utils.aggregation_constants import ALLOWED_CHANNEL_AGGREGATIONS
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import lock
from utils.celery.tasks import unlock

LOCK_NAME = 'cache_research_channels_aggs'

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.RESEARCH_CACHING, soft_time_limit=TaskTimeout.RESEARCH_CACHING)
def cache_channel_aggregations():
    try:
        lock(lock_name=LOCK_NAME, max_retries=60, expire=TaskExpiration.RESEARCH_CACHING)
        logger.debug("Starting channel aggregations caching.")
        sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                    Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY, Sections.CMS,
                    Sections.TASK_US_DATA, Sections.ANALYTICS, Sections.MONETIZATION)

        manager = ChannelManager(sections)

        aggregation_params = ALLOWED_CHANNEL_AGGREGATIONS

        cached_channel_aggregations, _ = CacheItem.objects.get_or_create(key=CHANNEL_AGGREGATIONS_KEY)

        logger.debug("Collecting channel aggregations.")
        aggregations = manager.get_aggregation(
            search=manager.search(filters=manager.forced_filters()),
            properties=aggregation_params
        )
        logger.debug("Saving channel aggregations.")
        cached_channel_aggregations.value = aggregations
        cached_channel_aggregations.save()
        logger.debug("Finished channel aggregations caching.")
        unlock(LOCK_NAME)
    except Exception as e:
        pass
