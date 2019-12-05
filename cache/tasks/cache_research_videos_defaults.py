import logging

from saas import celery_app
from es_components.constants import Sections
from es_components.managers.video import VideoManager

from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_cache import set_to_cache

from cache.models import CacheItem
from cache.constants import VIDEO_AGGREGATIONS_KEY

logger = logging.getLogger(__name__)

TIMEOUT = 14400


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
def cache_research_videos_defaults():
    logger.debug("Starting default research videos caching.")
    default_sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA, Sections.BRAND_SAFETY,
                        Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS, Sections.CMS,
                        Sections.CUSTOM_CAPTIONS)
    admin_sections = default_sections + (Sections.ANALYTICS,)

    fields_to_load = ['general_data', 'main', 'monetization', 'channel', 'analytics', 'ads_stats', 'captions',
                      'cms.cms_title', 'stats.subscribers', 'stats.last_video_published_at', 'stats.engage_rate',
                      'stats.sentiment', 'stats.views', 'stats.comments', 'stats.likes', 'stats.dislikes',
                      'stats.last_*_views', 'stats.last_*_likes', 'stats.views_per_video', 'general_data.country',
                      'stats.last_*_comments', 'stats.flags', 'stats.views_history', 'stats.likes_history',
                      'stats.dislikes_history', 'stats.comments_history', 'stats.historydate', 'brand_safety',
                      'custom_captions', 'general_data.iab_categories']

    sort = [
        {'stats.views': {'order': 'desc'}},
        {'main.id': {'order': 'asc'}}
    ]

    try:
        cached_aggregations_object, _ = CacheItem.objects.get_or_create(key=VIDEO_AGGREGATIONS_KEY)
        cached_aggregations = cached_aggregations_object.value
    except Exception as e:
        cached_aggregations = None

    # Caching for Default Sections (not Admin)
    manager = VideoManager(default_sections)
    queryset_adapter = ESQuerysetAdapter(manager, cached_aggregations=cached_aggregations)
    queryset_adapter.aggregations = []
    queryset_adapter.fields_to_load = fields_to_load
    queryset_adapter.filter_query = [manager.forced_filters()]
    queryset_adapter.percentiles = []
    queryset_adapter.sort = sort
    obj = queryset_adapter
    # Caching Count
    logger.debug("Caching default research videos count.")
    print("Caching default research videos count.")
    part = "count"
    update_cache(obj, part)
    # Caching Get_data
    logger.debug("Caching default research videos data.")
    print("Caching default research videos data.")
    part = "get_data"
    update_cache(obj, part)
    # Caching Count for Aggregations Query
    logger.debug("Caching default research videos aggregations count.")
    print("Caching default research videos aggregations count.")
    obj.sort = None
    part = "count"
    update_cache(obj, part)
    logger.debug("Finished default research videos caching.")
    print("Finished default research videos caching.")

    # Caching for Admin Sections
    admin_manager = VideoManager(admin_sections)
    admin_queryset_adapter = queryset_adapter
    admin_queryset_adapter.manager = admin_manager
    obj = admin_queryset_adapter
    # Caching Count
    logger.debug("Caching admin research videos count.")
    print("Caching admin research videos count.")
    part = "count"
    update_cache(obj, part)
    # Caching Get_data
    logger.debug("Caching admin research videos data.")
    print("Caching admin research videos data.")
    part = "get_data"
    update_cache(obj, part)
    # Caching Count for Aggregations Query
    logger.debug("Caching admin research videos aggregations count.")
    print("Caching admin research videos aggregations count.")
    obj.sort = None
    part = "count"
    update_cache(obj, part)
    logger.debug("Finished admin research videos caching.")
    print("Finished admin research videos caching.")
