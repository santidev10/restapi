import logging

from saas import celery_app
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.constants import FORCED_FILTER_OUDATED_DAYS
from es_components.constants import TimestampFields

from utils.es_components_cache import cached_method

forced_filter_oudated_days = FORCED_FILTER_OUDATED_DAYS
forced_filter_section_oudated = Sections.MAIN

logger = logging.getLogger(__name__)


def _filter_nonexistent_section(section):
    return QueryBuilder().build().must_not().exists().field(section).get()


def filter_alive():
    return _filter_nonexistent_section(Sections.GENERAL_DATA)


def _filter_existent_section(section):
    return QueryBuilder().build().must().exists().field(section).get()


def forced_filters():
    # "now-1d/d" time format is used
    # it avoids being tied to the current point in time and makes it possible to cache request/response
    outdated_seconds = forced_filter_oudated_days * 86400
    updated_at = f"now-{outdated_seconds}s/s"
    field_updated_at = f"{forced_filter_section_oudated}.{TimestampFields.UPDATED_AT}"
    filter_range = QueryBuilder().build().must().range().field(field_updated_at) \
        .gt(updated_at).get()
    return filter_alive() & filter_range & _filter_existent_section(Sections.GENERAL_DATA)


class VideosCache:
    def __init__(self):
        self.manager = None
        self.filter_query = None
        self.sort = None
        self.fields_to_load = None
        self.cached_aggregations = None
        self.aggregations = None

    @cached_method(timeout=14400)
    def count(self):
        count = self.manager.search(filters=self.filter_query).count()
        return count

    @cached_method(timeout=14400)
    def get_data(self, start=0, end=None):
        data = self.manager.search(
            filters=self.filter_query,
            sort=self.sort,
            offset=start,
            limit=end,
        ) \
            .source(includes=self.fields_to_load).execute().hits
        return data

    @cached_method(timeout=14400)
    def get_aggregations(self):
        if self.cached_aggregations and self.aggregations:
            aggregations = {aggregation: self.cached_aggregations[aggregation]
                            for aggregation in self.cached_aggregations
                            if aggregation in self.aggregations}
            return aggregations
        aggregations = self.manager.get_aggregation(
            search=self.manager.search(filters=self.filter_query),
            properties=self.aggregations,
        )
        return aggregations


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

    cache = VideosCache()
    manager = VideoManager(sections)

    cache.manager = manager
    cache.filter_query = forced_filters()
    cache.sort = sort
    cache.fields_to_load = fields_to_load

    logger.debug("Querying default research videos counts, filters, and aggregations for caching.")
    print("Querying default research videos counts, filters, and aggregations for caching.")
    cache.count()
    cache.get_data(start=0, end=50)
    cache.get_aggregations()

    logger.debug("Finished default research videos caching.")
    print("Finished default research videos caching.")

# <class 'list'>: [Bool(minimum_should_match=1, must=[Range(general_data__updated_at={'gt': 'now-604800s/s'}), Exists(field='general_data')], must_not=[Exists(field='deleted')], should=[Bool(must=[Exists(field='cms')]), Bool(must=[Exists(field='auth')]), Bool(must=[Range(stats__observed_videos_count={'gt': 0})])])]