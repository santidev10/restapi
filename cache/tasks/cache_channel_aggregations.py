import logging

from saas import celery_app
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from es_components.query_builder import QueryBuilder
from es_components.constants import FORCED_FILTER_OUDATED_DAYS
from es_components.constants import TimestampFields

from cache.constants import CHANNEL_AGGREGATIONS_KEY

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
def cache_channel_aggregations():
    logger.debug("Starting channel aggregations caching.")
    sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY, Sections.CMS,
                Sections.TASK_US_DATA)

    manager = ChannelManager(sections)

    aggregation_params = [
        "ads_stats.average_cpv:max",
        "ads_stats.average_cpv:min",
        "ads_stats.ctr_v:max",
        "ads_stats.ctr_v:min",
        "ads_stats.video_view_rate:max",
        "ads_stats.video_view_rate:min",
        "ads_stats:exists",
        "analytics.age13_17:max",
        "analytics.age13_17:min",
        "analytics.age18_24:max",
        "analytics.age18_24:min",
        "analytics.age25_34:max",
        "analytics.age25_34:min",
        "analytics.age35_44:max",
        "analytics.age35_44:min",
        "analytics.age45_54:max",
        "analytics.age45_54:min",
        "analytics.age55_64:max",
        "analytics.age55_64:min",
        "analytics.age65_:max",
        "analytics.age65_:min",
        "cms.cms_title",
        "analytics.gender_female:max",
        "analytics.gender_female:min",
        "analytics.gender_male:max",
        "analytics.gender_male:min",
        "analytics.gender_other:max",
        "analytics.gender_other:min",
        "analytics:exists",
        "analytics:missing",
        "general_data.emails:exists",
        "general_data.emails:missing",
        "custom_properties.preferred",
        "general_data.country",
        "general_data.top_category",
        "general_data.top_language",
        "general_data.iab_categories",
        "social.facebook_likes:max",
        "social.facebook_likes:min",
        "social.instagram_followers:max",
        "social.instagram_followers:min",
        "social.twitter_followers:max",
        "social.twitter_followers:min",
        "stats.last_30day_subscribers:max",
        "stats.last_30day_subscribers:min",
        "stats.last_30day_views:max",
        "stats.last_30day_views:min",
        "stats.subscribers:max",
        "stats.subscribers:min",
        "stats.views_per_video:max",
        "stats.views_per_video:min",
        "brand_safety",
        "stats.channel_group"
    ]

    cached_channel_aggregations, _ = CacheItem.objects.get_or_create(key=CHANNEL_AGGREGATIONS_KEY)

    forced_filter = forced_filters()

    logger.debug("Collecting channel aggregations.")
    print("Collecting channel aggregations.")
    aggregations = manager.get_aggregation(
        search=manager.search(filters=forced_filter),
        properties=aggregation_params
    )
    logger.debug("Saving channel aggregations.")
    cached_channel_aggregations.value = aggregations
    cached_channel_aggregations.save()
    logger.debug("Finished channel aggregations caching.")
    print("Finished channel aggregations caching.")
