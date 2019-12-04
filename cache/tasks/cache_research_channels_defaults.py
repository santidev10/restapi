import logging

from utils.redis import get_redis_client

from saas import celery_app
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager

from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_cache import set_to_cache

from cache.models import CacheItem
from cache.constants import CHANNEL_AGGREGATIONS_KEY

logger = logging.getLogger(__name__)

redis = get_redis_client()

TIMEOUT = 21400


def update_cache(obj, part, timeout=TIMEOUT):
    if part == "count":
        options = ((), {})
        data = obj.uncached_count()
    elif part == "get_data":
        options = ((0, 50), {})
        data = obj.uncached_get_data(0, 50)
    else:
        return
    set_to_cache(obj, part, options, data, timeout)


@celery_app.task()
def cache_research_channels_defaults():
    logger.debug("Starting default research channels caching.")
    default_sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY, Sections.CMS,
                Sections.TASK_US_DATA)
    admin_sections = default_sections + (Sections.MONETIZATION, Sections.ANALYTICS,)

    fields_to_load = ['main', 'social', 'custom_properties', 'ads_stats', 'analytics.updated_at', 'analytics.cms_title',
                      'analytics.traffic_source', 'analytics.age', 'analytics.country', 'analytics.gender',
                      'analytics.audience', 'cms.cms_title', 'general_data.title', 'general_data.description',
                      'general_data.thumbnail_image_url', 'general_data.country', 'general_data.youtube_published_at',
                      'general_data.top_category', 'general_data.top_language', 'general_data.emails',
                      'general_data.iab_categories', 'stats.subscribers', 'stats.last_video_published_at',
                      'stats.engage_rate', 'stats.sentiment', 'stats.last_30day_subscribers', 'stats.views',
                      'stats.last_*_views', 'stats.views_per_video', 'stats.channel_group', 'stats.subscribers_history',
                      'stats.views_history', 'stats.historydate', 'brand_safety', 'stats.channel_group',
                      'monetization.is_monetizable']

    sort = [
        {'stats.subscribers': {'order': 'desc'}},
        {'main.id': {'order': 'asc'}}
    ]

    try:
        cached_aggregations_object, _ = CacheItem.objects.get_or_create(key=CHANNEL_AGGREGATIONS_KEY)
        cached_aggregations = cached_aggregations_object.value
    except Exception as e:
        cached_aggregations = None

    # Caching for Default Sections (not Admin)
    manager = ChannelManager(default_sections)
    queryset_adapter = ESQuerysetAdapter(manager, cached_aggregations=cached_aggregations)
    queryset_adapter.aggregations = []
    queryset_adapter.fields_to_load = fields_to_load
    queryset_adapter.filter_query = [manager.forced_filters()]
    queryset_adapter.percentiles = []
    queryset_adapter.sort = sort

    logger.debug("Caching default research channels count and filters.")
    print("Caching default research channels count and filters.")

    obj = queryset_adapter
    # Caching Count
    part = "count"
    update_cache(obj, part)
    # Caching Get_data
    part = "get_data"
    update_cache(obj, part)

    logger.debug("Finished default research channels caching.")
    print("Finished default research channels caching.")

    # Caching for Admin Sections
    admin_manager = ChannelManager(admin_sections)
    admin_queryset_adapter = queryset_adapter
    admin_queryset_adapter.manager = admin_manager

    logger.debug("Caching admin research channels count and filters.")
    print("Caching admin research channels count and filters.")

    obj = admin_queryset_adapter
    # Caching Count
    part = "count"
    update_cache(obj, part)
    # Caching Get_data
    part = "get_data"
    update_cache(obj, part)

    logger.debug("Finished admin research channels caching.")
    print("Finished admin research channels caching.")