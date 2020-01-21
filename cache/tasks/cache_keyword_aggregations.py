import logging

from saas import celery_app
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.keyword import KeywordManager

from cache.constants import KEYWORD_AGGREGATIONS_KEY
from utils.aggregation_constants import ALLOWED_KEYWORD_AGGREGATIONS
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.RESEARCH_CACHING, soft_time_limit=TaskTimeout.RESEARCH_CACHING)
def cache_keyword_aggregations():
    logger.debug("Starting keyword aggregations caching.")
    sections = (Sections.MAIN, Sections.STATS,)

    manager = KeywordManager(sections)

    aggregation_params = ALLOWED_KEYWORD_AGGREGATIONS

    cached_keyword_aggregations, _ = CacheItem.objects.get_or_create(key=KEYWORD_AGGREGATIONS_KEY)

    logger.debug("Collecting keyword aggregations.")
    aggregations = manager.get_aggregation(
        search=manager.search(filters=manager.forced_filters()),
        properties=aggregation_params
    )
    logger.debug("Saving keyword aggregations.")
    cached_keyword_aggregations.value = aggregations
    cached_keyword_aggregations.save()
    logger.debug("Finished keyword aggregations caching.")
