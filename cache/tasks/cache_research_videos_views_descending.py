import logging

from saas import celery_app
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.constants import FORCED_FILTER_OUDATED_DAYS
from es_components.constants import TimestampFields

from cache.constants import RESEARCH_VIDEOS_KEY

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
def cache_research_videos_views_descending():
    logger.debug("Starting research videos caching, sorted by descending views.")
    sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA, Sections.BRAND_SAFETY,
                Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS, Sections.CMS,
                Sections.CUSTOM_CAPTIONS)

    manager = VideoManager(sections)

    forced_filter = forced_filters()
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

    cached_research_videos, _ = CacheItem.objects.get_or_create(key=RESEARCH_VIDEOS_KEY)

    logger.debug("Querying research videos, sorted by descending views.")
    print("Querying research videos, sorted by descending views.")
    data = manager.search(
        filters=forced_filter,
        sort=sort,
        offset=0,
        limit=50,
    )\
        .source(includes=fields_to_load).execute().hits
    cached_research_videos.value = data
    cached_research_videos.save()
    logger.debug("Finished research videos caching, sorted by descending views.")
    print("Finished research videos caching, sorted by descending views.")
