from datetime import timedelta
import logging

from django.db.models import Q
from django.utils import timezone

from saas import celery_app
from segment.tasks.generate_segment import generate_segment
from segment.models import CustomSegmentFileUpload
from utils.celery.tasks import unlock
from utils.celery.tasks import REDIS_CLIENT

logger = logging.getLogger(__name__)

UPDATE_THRESHOLD = 7
LOCK_NAME = "update_custom_segment"
EXPIRE = 60 * 60 * 2


@celery_app.task
def update_custom_segment():
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, EXPIRE).acquire(blocking=False)
    if is_acquired:
        threshold = timezone.now() - timedelta(days=UPDATE_THRESHOLD)
        export_to_update = CustomSegmentFileUpload.objects.filter(
            (Q(updated_at__isnull=True) & Q(created_at__lte=threshold)) | Q(updated_at__lte=threshold)
        ).first()
        if export_to_update:
            segment = export_to_update.segment
            results = generate_segment(segment, export_to_update.query, segment.LIST_SIZE)
            segment.statistics = results["statistics"]
            export_to_update.download_url = results["download_url"]
            export_to_update.updated_at = timezone.now()
            export_to_update.save()
            segment.save()
            logger.info(f"Successfully updated export for custom list: id: {segment.id}, title: {segment.title}")
        unlock(LOCK_NAME, fail_silently=False)
