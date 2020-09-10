import logging

from cache.constants import ADMIN_VIDEO_AGGREGATIONS_KEY
from cache.constants import VIDEO_AGGREGATIONS_KEY
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.video import VettingAdminVideoManager
from es_components.managers.video import VideoManager
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.aggregation_constants import ALLOWED_VIDEO_AGGREGATIONS
from utils.celery.tasks import lock
from utils.celery.tasks import unlock

LOCK_NAME = "cache_research_videos_aggs"

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.RESEARCH_CACHING, soft_time_limit=TaskTimeout.RESEARCH_CACHING)
def cache_video_aggregations():
    try:
        # pylint: disable=no-value-for-parameter
        lock(lock_name=LOCK_NAME, max_retries=60, expire=TaskExpiration.RESEARCH_CACHING)
        # pylint: enable=no-value-for-parameter
        logger.info("Starting video aggregations caching.")
        sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA, Sections.BRAND_SAFETY,
                    Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS, Sections.CMS,
                    Sections.CUSTOM_CAPTIONS, Sections.ANALYTICS)
        aggregation_params = ALLOWED_VIDEO_AGGREGATIONS

        for key, manager_class in {
            ADMIN_VIDEO_AGGREGATIONS_KEY: VettingAdminVideoManager,
            VIDEO_AGGREGATIONS_KEY: VideoManager,
        }.items():
            cached_video_aggregations, _ = CacheItem.objects.get_or_create(key=key)

            logger.info(f"Collecting video aggregations for key, '{key}'.")
            manager = manager_class(sections)
            aggregations = manager.get_aggregation(
                search=manager.search(filters=manager.forced_filters()),
                properties=aggregation_params
            )
            logger.info(f"Saving video aggregations for key, '{key}'.")
            cached_video_aggregations.value = aggregations
            cached_video_aggregations.save()

        logger.info("Finished video aggregations caching.")
        unlock(LOCK_NAME)
    # pylint: disable=broad-except
    except Exception:
        # pylint: enable=broad-except
        pass
