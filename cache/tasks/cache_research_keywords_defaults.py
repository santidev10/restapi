import logging

from saas import celery_app
from es_components.constants import Sections
from es_components.managers.keyword import KeywordManager

from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_cache import set_to_cache

from cache.models import CacheItem
from cache.constants import KEYWORD_AGGREGATIONS_KEY

logger = logging.getLogger(__name__)

TIMEOUT = 14400


def update_cache(obj, part, options=None, timeout=TIMEOUT):
    if part == "count":
        options = options or ((), {})
        data = obj.uncached_count()
    elif part == "get_data":
        options = options or ((0, 50), {})
        data = obj.uncached_get_data(0, 50)
    else:
        return
    set_to_cache(obj, part, options, data, timeout)


@celery_app.task()
def cache_research_keywords_defaults():
    logger.debug("Starting default research channels caching.")
    default_sections = (Sections.MAIN, Sections.STATS)

    fields_to_load = ['main', 'stats']

    sort = [
        {'stats.views': {'order': 'desc'}},
        {'main.id': {'order': 'asc'}}
    ]

    try:
        cached_aggregations_object, _ = CacheItem.objects.get_or_create(key=KEYWORD_AGGREGATIONS_KEY)
        cached_aggregations = cached_aggregations_object.value
    except Exception as e:
        cached_aggregations = None

    # Caching for Default Sections (not Admin)
    manager = KeywordManager(default_sections)
    queryset_adapter = ESQuerysetAdapter(manager, cached_aggregations=cached_aggregations)
    queryset_adapter.aggregations = []
    queryset_adapter.fields_to_load = fields_to_load
    queryset_adapter.filter_query = [manager.forced_filters()]
    queryset_adapter.percentiles = []
    queryset_adapter.sort = sort
    obj = queryset_adapter
    # Caching Count
    logger.debug("Caching default research keywords count.")
    print("Caching default research keywords count.")
    part = "count"
    update_cache(obj, part)
    # Caching Get_data
    logger.debug("Caching default research keywords data.")
    print("Caching default research keywords data.")
    part = "get_data"
    update_cache(obj, part)
    # Caching Count for Aggregations Query
    logger.debug("Caching default research keywords aggregations count.")
    print("Caching default research keywords aggregations count.")
    obj.sort = None
    part = "count"
    update_cache(obj, part)
    # Caching Data for Aggregations Query
    logger.debug("Caching default research keywords aggregations data.")
    print("Caching default research keywords aggregations data.")
    part = "get_data"
    update_cache(obj, part, options=((0, 0), {}))
    logger.debug("Finished default research keywords caching.")
    print("Finished default research keywords caching.")
