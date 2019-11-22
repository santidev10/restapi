import logging

from saas import celery_app
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.video import VideoManager

from cache.constants import VIDEO_AGGREGATIONS_KEY

logger = logging.getLogger(__name__)


@celery_app.task()
def cache_video_aggregations():
    logger.debug("Starting video aggregations caching.")
    sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA, Sections.BRAND_SAFETY,
                Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS, Sections.CMS,
                Sections.CUSTOM_CAPTIONS)

    manager = VideoManager(sections)

    aggregation_params = [
        "ads_stats.average_cpv:max",
        "ads_stats.average_cpv:min",
        "ads_stats.ctr_v:max",
        "ads_stats.ctr_v:min",
        "ads_stats.video_view_rate:max",
        "ads_stats.video_view_rate:min",
        "analytics:exists",
        "analytics:missing",
        "cms.cms_title",
        "general_data.category",
        "general_data.country",
        "general_data.iab_categories",
        "general_data.language",
        "general_data.youtube_published_at:max",
        "general_data.youtube_published_at:min",
        "stats.flags:exists",
        "stats.flags:missing",
        "stats.channel_subscribers:max",
        "stats.channel_subscribers:min",
        "stats.last_day_views:max",
        "stats.last_day_views:min",
        "stats.views:max",
        "stats.views:min",
        "brand_safety",
        "stats.flags",
        "custom_captions.items:exists",
        "custom_captions.items:missing",
        "captions:exists",
        "captions:missing",
        "stats.sentiment:max",
        "stats.sentiment:min"
    ]

    cached_video_aggregations, _ = CacheItem.objects.get_or_create(key=VIDEO_AGGREGATIONS_KEY)

    logger.debug("Collecting video aggregations.")
    print("Collecting video aggregations.")
    aggregations = manager.get_aggregation(
        search=manager.search(filters=manager.forced_filters()),
        properties=aggregation_params
    )
    logger.debug("Saving video aggregations.")
    cached_video_aggregations.value = aggregations
    cached_video_aggregations.save()
    logger.debug("Finished video aggregations caching.")
    print("Finished video aggregations caching.")
