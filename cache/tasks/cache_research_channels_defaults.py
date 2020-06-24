import logging

from saas import celery_app
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager

from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_cache import set_to_cache

from cache.models import CacheItem
from cache.constants import CHANNEL_AGGREGATIONS_KEY
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import lock
from utils.celery.tasks import unlock

LOCK_NAME = 'cache_research_channels_defaults'
logger = logging.getLogger(__name__)
TIMEOUT = 14400


def update_cache(obj, part, options=None, timeout=TIMEOUT):
    if part == "count":
        options = options or ((), {})
        data = obj.uncached_count()
    elif part == "get_data":
        options = options or ((0, 50), {})
        start = options[0][0]
        end = options[0][1]
        data = obj.uncached_get_data(start, end)
    else:
        return
    set_to_cache(obj, part, options, data, timeout)


@celery_app.task(expires=TaskExpiration.RESEARCH_CACHING, soft_time_limit=TaskTimeout.RESEARCH_CACHING)
def cache_research_channels_defaults():
    try:
        lock(lock_name=LOCK_NAME, max_retries=60, expire=TaskExpiration.RESEARCH_CACHING)
        logger.info("Starting default research channels caching.")
        default_sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                    Sections.CUSTOM_PROPERTIES, Sections.SOCIAL, Sections.BRAND_SAFETY, Sections.CMS,
                    Sections.TASK_US_DATA)
        admin_sections = default_sections + (Sections.MONETIZATION, Sections.ANALYTICS,)

        fields_to_load = ['main', 'social', 'custom_properties', 'ads_stats', 'cms.cms_title', 'general_data.title',
                          'general_data.description', 'general_data.thumbnail_image_url', 'general_data.country',
                          'general_data.youtube_published_at', 'general_data.top_category', 'general_data.top_language',
                          'general_data.emails', 'general_data.iab_categories', 'stats.subscribers',
                          'stats.last_video_published_at', 'stats.engage_rate', 'stats.sentiment',
                          'stats.last_30day_subscribers', 'stats.views', 'stats.last_*_views', 'stats.views_per_video',
                          'stats.channel_group', 'stats.subscribers_history', 'stats.views_history', 'stats.historydate',
                          'brand_safety']

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
        obj = queryset_adapter
        # Caching Count
        logger.info("Caching default research channels count.")
        part = "count"
        update_cache(obj, part)
        # Caching Get_data
        logger.info("Caching default research channels data.")
        part = "get_data"
        update_cache(obj, part)
        # Caching Count for Aggregations Query
        logger.info("Caching default research channels aggregations count.")
        obj.sort = None
        part = "count"
        update_cache(obj, part)
        # Caching Data for Aggregations Query
        logger.info("Caching default research channels aggregations data.")
        part = "get_data"
        update_cache(obj, part, options=((0, 0), {}))
        logger.info("Finished default research channels caching.")

        # Caching for Admin Sections
        admin_manager = ChannelManager(admin_sections)
        admin_queryset_adapter = queryset_adapter
        admin_queryset_adapter.manager = admin_manager
        admin_fields_to_load = ['main', 'social', 'custom_properties', 'ads_stats', 'analytics.updated_at', 'analytics.cms_title',
                                'analytics.traffic_source', 'analytics.age', 'analytics.country', 'analytics.gender',
                                'analytics.audience', 'cms.cms_title', 'general_data.title', 'general_data.description',
                                'general_data.thumbnail_image_url', 'general_data.country', 'general_data.youtube_published_at',
                                'general_data.top_category', 'general_data.top_language', 'general_data.emails',
                                'general_data.iab_categories', 'stats.subscribers', 'stats.last_video_published_at',
                                'stats.engage_rate', 'stats.sentiment', 'stats.last_30day_subscribers', 'stats.views',
                                'stats.last_*_views', 'stats.views_per_video', 'stats.channel_group', 'stats.subscribers_history',
                                'stats.views_history', 'stats.historydate', 'brand_safety', 'monetization.is_monetizable']
        admin_queryset_adapter.fields_to_load = admin_fields_to_load
        obj = admin_queryset_adapter
        obj.sort = sort
        # Caching Count
        logger.info("Caching admin research channels count.")
        part = "count"
        update_cache(obj, part)
        # Caching Get_data
        logger.info("Caching admin research channels data.")
        part = "get_data"
        update_cache(obj, part)
        # Caching Count for Aggregations Query
        logger.info("Caching admin research channels aggregations count.")
        obj.sort = None
        part = "count"
        update_cache(obj, part)
        # Caching Data for Aggregations Query
        logger.info("Caching admin research channels aggregations data.")
        part = "get_data"
        update_cache(obj, part, options=((0, 0), {}))
        logger.info("Finished admin research channels caching.")
        unlock(LOCK_NAME)
    except Exception as e:
        pass
