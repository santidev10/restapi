import logging

from saas import celery_app
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.constants import FORCED_FILTER_OUDATED_DAYS
from es_components.constants import TimestampFields

from cache.constants import VIDEO_AGGREGATIONS_KEY

forced_filter_oudated_days = FORCED_FILTER_OUDATED_DAYS
forced_filter_section_oudated = Sections.MAIN

logger = logging.getLogger(__name__)

def _filter_nonexistent_section(section):
    return QueryBuilder().build().must_not().exists().field(section).get()

def filter_alive():
    return _filter_nonexistent_section(Sections.DELETED)

def forced_filters():
    # "now-1d/d" time format is used
    # it avoids being tied to the current point in time and makes it possible to cache request/response
    outdated_seconds = forced_filter_oudated_days * 86400
    updated_at = f"now-{outdated_seconds}s/s"
    field_updated_at = f"{forced_filter_section_oudated}.{TimestampFields.UPDATED_AT}"
    filter_range = QueryBuilder().build().must().range().field(field_updated_at) \
        .gt(updated_at).get()
    return filter_alive() & filter_range


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

    forced_filter = forced_filters()

    logger.debug("Collecting video aggregations.")
    print("Collecting video aggregations.")
    aggregations = manager.get_aggregation(
        search=manager.search(filters=forced_filter),
        properties=aggregation_params
    )
    logger.debug("Saving video aggregations.")
    cached_video_aggregations.value = aggregations
    cached_video_aggregations.save()
    logger.debug("Finished video aggregations caching.")
    print("Finished video aggregations caching.")
