import logging

from saas import celery_app
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.video import VideoManager

from cache.constants import VIDEO_AGGREGATIONS_KEY
from utils.aggregation_constants import ALLOWED_VIDEO_AGGREGATIONS
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.RESEARCH_CACHING, soft_time_limit=TaskTimeout.RESEARCH_CACHING)
def cache_video_aggregations():
    logger.debug("Starting video aggregations caching.")
    sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA, Sections.BRAND_SAFETY,
                Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS, Sections.CMS,
                Sections.CUSTOM_CAPTIONS, Sections.ANALYTICS)

    manager = VideoManager(sections)

    aggregation_params = ALLOWED_VIDEO_AGGREGATIONS

    cached_video_aggregations, _ = CacheItem.objects.get_or_create(key=VIDEO_AGGREGATIONS_KEY)

    logger.debug("Collecting video aggregations.")
    aggregations = manager.get_aggregation(
        search=manager.search(filters=manager.forced_filters()),
        properties=aggregation_params
    )
    logger.debug("Saving video aggregations.")
    cached_video_aggregations.value = aggregations
    cached_video_aggregations.save(update_fields=['value'])
    logger.debug("Finished video aggregations caching.")
