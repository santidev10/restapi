import logging

from saas import celery_app
from es_components.constants import Sections
from es_components.managers.video import VideoManager

from utils.es_components_api_utils import ESQuerysetAdapter

from cache.models import CacheItem
from cache.constants import VIDEO_AGGREGATIONS_KEY

logger = logging.getLogger(__name__)


@celery_app.task()
def cache_research_videos_defaults():
    logger.debug("Starting default research videos caching.")
    sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA, Sections.BRAND_SAFETY,
                Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS, Sections.CMS,
                Sections.CUSTOM_CAPTIONS)

    fields_to_load = ['general_data', 'main', 'monetization', 'channel', 'analytics', 'ads_stats',
                      'captions', 'cms.cms_title', 'stats.subscribers', 'stats.last_video_published_at',
                      'stats.engage_rate', 'stats.sentiment', 'stats.views', 'stats.comments', 'stats.likes',
                      'stats.dislikes', 'stats.last_*_views', 'stats.last_*_likes', 'stats.views_per_video',
                      'general_data.country', 'stats.last_*_comments', 'stats.flags', 'stats.views_history',
                      'stats.likes_history', 'stats.dislikes_history', 'stats.comments_history', 'stats.historydate',
                      'brand_safety', 'custom_captions', 'general_data.iab_categories']
    sort = [
        {'stats.views': {'order': 'desc'}},
        {'main.id': {'order': 'asc'}}
    ]

    try:
        cached_aggregations_object, _ = CacheItem.objects.get_or_create(key=VIDEO_AGGREGATIONS_KEY)
        cached_aggregations = cached_aggregations_object.value
    except Exception as e:
        cached_aggregations = None

    manager = VideoManager(sections)

    queryset_adapter = ESQuerysetAdapter(manager, cached_aggregations=cached_aggregations)

    queryset_adapter.aggregations = []
    queryset_adapter.fields_to_load = fields_to_load
    queryset_adapter.filter_query = [manager.forced_filters()]
    queryset_adapter.percentiles = []
    queryset_adapter.sort = sort

    logger.debug("Caching default research videos count and filters.")
    print("Caching default research videos count and filters.")
    queryset_adapter.count()
    queryset_adapter.get_data(0, 50)

    logger.debug("Finished default research videos caching.")
    print("Finished default research videos caching.")