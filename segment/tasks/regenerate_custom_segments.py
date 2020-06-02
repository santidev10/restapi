from datetime import timedelta
from django.db.models import Q
from django.utils import timezone
from saas import celery_app
from segment.models import CustomSegment
from segment.tasks.generate_segment import generate_segment
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock
import logging

LOCK_NAME = "regenerate_custom_segments"
EXPIRE = 60 * 60 * 24 * 2
DAYS_THRESHOLD = 7

logger = logging.getLogger(__name__)


@celery_app.task
def regenerate_custom_segments_with_lock():
    """
    run regeneration with lock
    """
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, EXPIRE).acquire(blocking=False)
    if is_acquired:
        try:
            regenerate_custom_segments()
        except Exception as e:
            logger.exception("Error in regenerate_custom_segments_with_lock task")
        finally:
            unlock(LOCK_NAME, fail_silently=True)


def regenerate_custom_segments():
    """
    regenerate export for segments flagged as `is_regenerating` and last
    updated at least DAYS_THRESHOLD ago. These custom segments are the new
    persistent segments. Typically, an is_featured segment is also an
    is_regenerating segment, but we're separating the functionality just
    in case. These are the new Brand Safety Target Lists. All non-featured
    segments are Custom Target Lists.
    """
    date_threshold = timezone.now() - timedelta(days=DAYS_THRESHOLD)
    for segment in CustomSegment.objects.filter(
            Q(is_regenerating=True) & Q(updated_at__lte=date_threshold)
    ):
        logger.debug(f"Processing regenerating segment titled: {segment.title}")
        export = segment.export
        results = generate_segment(segment, export.query["body"], segment.LIST_SIZE, add_uuid=True)
        segment.statistics = results["statistics"]
        segment.save()
        export.download_url = results["download_url"]
        export.completed_at = timezone.now()
        export.save()
