import logging

from django.contrib.auth import get_user_model

from aw_reporting.api.views.trends.base_global_trends import get_account_queryset
from aw_reporting.tools.trends_tool.global_filters import GlobalTrendsFilters
from cache.constants import GLOBAL_TRENDS_FILTERS_KEY
from cache.models import CacheItem
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.GLOBAL_TRENDS_FILTERS_CACHING,
                 soft_time_limit=TaskTimeout.GLOBAL_TRENDS_FILTERS_CACHING)
def cache_global_trends_filters():
    logger.debug("Starting global trends filters caching.")

    for user in get_user_model().objects.filter(is_active=True).all():
        accounts = get_account_queryset(user)

        if not accounts.exists():
            continue

        global_trends_filters = GlobalTrendsFilters().get_filters(user=user)

        cached_pricing_tool_filters, _ = CacheItem.objects.get_or_create(key=f"{user.id}_{GLOBAL_TRENDS_FILTERS_KEY}")

        logger.debug("Saving keyword aggregations for user with id %s.", user.id)
        cached_pricing_tool_filters.value = global_trends_filters
        cached_pricing_tool_filters.save()
    logger.debug("Finished keyword aggregations caching.")
