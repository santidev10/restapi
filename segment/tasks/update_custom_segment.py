import logging
from datetime import timedelta

from django.db.models import Q
from django.utils import timezone

from saas import celery_app
from segment.models import CustomSegmentFileUpload
from segment.utils.generate_segment import generate_segment
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock

logger = logging.getLogger(__name__)

UPDATE_THRESHOLD = 7
LOCK_NAME = "update_custom_segment"
EXPIRE = 60 * 60 * 2


@celery_app.task
def update_custom_segment():
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, EXPIRE).acquire(blocking=False)
    if is_acquired:
        try:
            threshold = timezone.now() - timedelta(days=UPDATE_THRESHOLD)
            export_to_update = CustomSegmentFileUpload.objects.filter(
                (Q(updated_at__isnull=True) & Q(created_at__lte=threshold)) | Q(updated_at__lte=threshold)
            ).first()
            if export_to_update:
                segment = export_to_update.segment
                size = segment.config.ADMIN_LIST_SIZE
                results = generate_segment(segment, export_to_update.query["body"], size)
                segment.statistics = results["statistics"]
                export_to_update.download_url = results["download_url"]
                export_to_update.updated_at = timezone.now()
                export_to_update.save()
                segment.save()
                logger.info("Successfully updated export for custom list: id: %s, title: %s", segment.id, segment.title)
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            logger.exception("Error in update_custom_segment task")
        finally:
            unlock(LOCK_NAME, fail_silently=True)
