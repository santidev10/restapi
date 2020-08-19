import logging

from cache.models import CacheItem
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from aw_reporting.calculations.pacing_report_filters import get_pacing_report_filters
from cache.constants import PACING_REPORT_FILTERS_KEY
from django.utils import timezone

LOCK_NAME = "cache_pacing_report_filters"

MAX_RETRIES = 5

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.PACING_REPORT_FILTERS, soft_time_limit=TaskTimeout.PACING_REPORT_FILTERS)
def cache_channel_aggregations():
    try:
        # pylint: disable=no-value-for-parameter
        lock(lock_name=LOCK_NAME, max_retries=MAX_RETRIES, expire=TaskExpiration.PACING_REPORT_FILTERS)
        # pylint: enable=no-value-for-parameter
        logger.info("Starting Pacing Report filters caching.")

        filters = get_pacing_report_filters()

        logger.info("Updating or creating Pacing Report filters cache.")
        CacheItem.objects.update_or_create(
            key=PACING_REPORT_FILTERS_KEY,
            defaults={
                'updated_at': timezone.now(),
                'value': filters,
            }
        )

        logger.info("Finished Pacing Report filters caching.")
        unlock(LOCK_NAME)
    # pylint: disable=broad-except
    except Exception as e:
        # pylint: enable=broad-except
        logger.exception(f"Pacing Report caching exception: {e}")
        pass
