import logging

from aw_reporting.tools.forecast_tool.forecast_tool import ForecastTool
from cache.constants import FORECAST_TOOL_FILTERS_KEY
from cache.models import CacheItem
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.FORECAST_TOOL_FILTERS_CACHING,
                 soft_time_limit=TaskTimeout.FORECAST_TOOL_FILTERS_CACHING)
def cache_forecast_tool_filters():
    logger.debug("Starting forecast tool filters caching.")

    forecast_tool_filters = ForecastTool.get_filters()

    cached_pricing_tool_filters, _ = CacheItem.objects.get_or_create(key=FORECAST_TOOL_FILTERS_KEY)

    cached_pricing_tool_filters.value = forecast_tool_filters
    cached_pricing_tool_filters.save()
    logger.debug("Finished forecast tool caching.")
