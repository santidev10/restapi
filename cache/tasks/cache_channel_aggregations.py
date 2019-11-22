import logging

from saas import celery_app
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager

from cache.constants import CHANNEL_AGGREGATIONS_KEY
from utils.aggregation_constants import ALLOWED_CHANNEL_AGGREGATIONS

logger = logging.getLogger(__name__)


@celery_app.task()
def cache_channel_aggregations():
    logger.debug("Starting channel aggregations caching.")
    sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY, Sections.CMS,
                Sections.TASK_US_DATA)

    manager = ChannelManager(sections)

    aggregation_params = ALLOWED_CHANNEL_AGGREGATIONS

    cached_channel_aggregations, _ = CacheItem.objects.get_or_create(key=CHANNEL_AGGREGATIONS_KEY)

    logger.debug("Collecting channel aggregations.")
    print("Collecting channel aggregations.")
    aggregations = manager.get_aggregation(
        search=manager.search(filters=manager.forced_filters()),
        properties=aggregation_params
    )
    logger.debug("Saving channel aggregations.")
    cached_channel_aggregations.value = aggregations
    cached_channel_aggregations.save()
    logger.debug("Finished channel aggregations caching.")
    print("Finished channel aggregations caching.")
