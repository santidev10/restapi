import logging

from saas import celery_app
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager

from utils.es_components_api_utils import ESQuerysetAdapter

from cache.models import CacheItem
from cache.constants import CHANNEL_AGGREGATIONS_KEY

logger = logging.getLogger(__name__)


@celery_app.task()
def cache_research_channels_defaults():
    logger.debug("Starting default research channels caching.")
    sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY, Sections.CMS,
                Sections.TASK_US_DATA)

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

    manager = ChannelManager(sections)

    queryset_adapter = ESQuerysetAdapter(manager, cached_aggregations=cached_aggregations)

    queryset_adapter.aggregations = []
    queryset_adapter.fields_to_load = fields_to_load
    queryset_adapter.filter_query = [manager.forced_filters()]
    queryset_adapter.percentiles = []
    queryset_adapter.sort = sort

    logger.debug("Caching default research channels count and filters.")
    print("Caching default research channels count and filters.")
    queryset_adapter.count()
    queryset_adapter.get_data(0, 50)

    logger.debug("Finished default research channels caching.")
    print("Finished default research channels caching.")
