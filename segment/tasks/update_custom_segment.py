from datetime import timedelta

from django.db.models import Q
from django.utils import timezone

from saas import celery_app
from segment.tasks.generate_segment import generate_segment
from segment.models import CustomSegmentFileUpload
from utils.celery.tasks import unlock
from utils.celery.tasks import REDIS_CLIENT

UPDATE_THRESHOLD = 7
LOCK_NAME = "update_custom_segment"
EXPIRE = 60 * 60


@celery_app.task
def update_custom_segment():
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, EXPIRE).acquire(blocking=False)
    if is_acquired:
        threshold = timezone.now() - timedelta(days=UPDATE_THRESHOLD)
        export_to_update = CustomSegmentFileUpload.objects.filter(
            (Q(updated_at__isnull=True) & Q(created_at__lte=threshold)) | Q(updated_at__lte=threshold)
        ).first()
        segment = export_to_update.segment
        results = generate_segment(segment, export_to_update.query, segment.LIST_SIZE)
        export_to_update.download_url = results["download_url"]
        segment.statistics = results["statistics"]
        unlock(LOCK_NAME, fail_silently=True)

