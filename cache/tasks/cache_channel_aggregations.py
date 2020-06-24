import logging

from cache.constants import CHANNEL_AGGREGATIONS_KEY
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.aggregation_constants import ALLOWED_CHANNEL_AGGREGATIONS
from utils.celery.tasks import lock
from utils.celery.tasks import unlock

LOCK_NAME = "cache_research_channels_aggs"

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.RESEARCH_CACHING, soft_time_limit=TaskTimeout.RESEARCH_CACHING)
def cache_channel_aggregations():
    try:
        # pylint: disable=no-value-for-parameter
        lock(lock_name=LOCK_NAME, max_retries=60, expire=TaskExpiration.RESEARCH_CACHING)
        # pylint: enable=no-value-for-parameter
        logger.info("Starting channel aggregations caching.")
        sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                    Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY, Sections.CMS,
                    Sections.TASK_US_DATA, Sections.ANALYTICS, Sections.MONETIZATION)

        manager = ChannelManager(sections)

        aggregation_params = ALLOWED_CHANNEL_AGGREGATIONS

        cached_channel_aggregations, _ = CacheItem.objects.get_or_create(key=CHANNEL_AGGREGATIONS_KEY)

        logger.info("Collecting channel aggregations.")
        aggregations = manager.get_aggregation(
            search=manager.search(filters=manager.forced_filters()),
            properties=aggregation_params
        )
        logger.info("Saving channel aggregations.")
        cached_channel_aggregations.value = aggregations
        cached_channel_aggregations.save()
        logger.info("Finished channel aggregations caching.")
        unlock(LOCK_NAME)
    # pylint: disable=broad-except
    except Exception:
        # pylint: enable=broad-except
        pass
