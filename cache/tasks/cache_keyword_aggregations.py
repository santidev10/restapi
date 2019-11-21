import logging

from saas import celery_app
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.keyword import KeywordManager
from es_components.query_builder import QueryBuilder
from es_components.constants import FORCED_FILTER_OUDATED_DAYS
from es_components.constants import TimestampFields

from cache.constants import KEYWORD_AGGREGATIONS_KEY

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
def cache_keyword_aggregations():
    logger.debug("Starting keyword aggregations caching.")
    sections = (Sections.MAIN, Sections.STATS,)

    manager = KeywordManager(sections)

    aggregation_params = [
        "stats.search_volume:min",
        "stats.search_volume:max",
        "stats.average_cpc:min",
        "stats.average_cpc:max",
        "stats.competition:min",
        "stats.competition:max",
        "stats.is_viral"
    ]

    cached_keyword_aggregations, _ = CacheItem.objects.get_or_create(key=KEYWORD_AGGREGATIONS_KEY)

    forced_filter = forced_filters()

    logger.debug("Collecting keyword aggregations.")
    print("Collecting keyword aggregations.")
    aggregations = manager.get_aggregation(
        search=manager.search(filters=forced_filter),
        properties=aggregation_params
    )
    logger.debug("Saving keyword aggregations.")
    cached_keyword_aggregations.value = aggregations
    cached_keyword_aggregations.save()
    logger.debug("Finished keyword aggregations caching.")
    print("Finished keyword aggregations caching.")
