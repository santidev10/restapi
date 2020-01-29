import logging

from django.contrib.auth import get_user_model

from aw_reporting.tools.pricing_tool import PricingTool
from saas import celery_app
from cache.models import CacheItem
from cache.constants import PRICING_TOOL_FILTERS_KEY
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.RESEARCH_CACHING, soft_time_limit=TaskTimeout.RESEARCH_CACHING)
def cache_pricing_tool_filters():
    logger.debug("Starting pricing tool filters caching.")

    for user in get_user_model().objects.all():
        pricing_tool_filters = PricingTool.get_filters(user=user)

        cached_pricing_tool_filters, _ = CacheItem.objects.get_or_create(key=f"{user.id}_{PRICING_TOOL_FILTERS_KEY}")

        logger.debug("Saving keyword aggregations for user with id %s.", user.id)
        cached_pricing_tool_filters.value = pricing_tool_filters
        cached_pricing_tool_filters.save()
    logger.debug("Finished keyword aggregations caching.")