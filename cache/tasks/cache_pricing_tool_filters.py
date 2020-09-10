import logging

from django.contrib.auth import get_user_model

from aw_reporting.models import Opportunity
from aw_reporting.tools.pricing_tool import PricingTool
from cache.constants import PRICING_TOOL_FILTERS_KEY
from cache.models import CacheItem
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.PRICING_TOOL_FILTERS_CACHING,
                 soft_time_limit=TaskTimeout.PRICING_TOOL_FILTERS_CACHING)
def cache_pricing_tool_filters():
    logger.debug("Starting pricing tool filters caching.")

    PricingTool.clean_filters_defaults()
    campaign_all_count = Opportunity.objects.have_campaigns().count()
    campaign_all_filters = PricingTool.get_filters()

    for user in get_user_model().objects.filter(is_active=True).all():
        opportunities_ids = Opportunity.objects.have_campaigns(user=user).values_list("id", flat=True)

        if not len(opportunities_ids) > 0:
            continue

        if campaign_all_count == len(opportunities_ids):
            pricing_tool_filters = campaign_all_filters
        else:
            pricing_tool_filters = PricingTool.get_filters(user=user)

        cached_pricing_tool_filters, _ = CacheItem.objects.get_or_create(key=f"{user.id}_{PRICING_TOOL_FILTERS_KEY}")

        logger.debug("Saving keyword aggregations for user with id %s.", user.id)
        cached_pricing_tool_filters.value = pricing_tool_filters
        cached_pricing_tool_filters.save()
    logger.debug("Finished keyword aggregations caching.")
