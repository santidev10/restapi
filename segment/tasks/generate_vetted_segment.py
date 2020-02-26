import logging

from django.utils import timezone

from saas import celery_app
from segment.models import CustomSegment
from segment.models import CustomSegmentVettedFileUpload
from segment.tasks.generate_segment import generate_segment
from utils.celery.tasks import unlock
from utils.celery.tasks import REDIS_CLIENT


LOCK_NAME = "segments.generate_vetted_segments"
EXPIRE = 60 * 60 * 1

logger = logging.getLogger(__name__)


@celery_app.task
def generate_vetted_segment():
    try:
        is_acquired = REDIS_CLIENT.lock(LOCK_NAME, EXPIRE).acquire(blocking=False)
        if is_acquired:
            completed_vetting_segments = CustomSegment.objects.filter(audit_id__isnull=False, is_vetting_complete=True)
            to_generate = [item for item in completed_vetting_segments if not hasattr(item, "vetted_export")]
            for segment in to_generate:
                try:
                    query = segment.get_vetted_items_query()
                    s3_key = segment.get_vetted_s3_key()
                    results = generate_segment(segment, query, segment.LIST_SIZE, add_uuid=False, s3_key=s3_key)
                    vetted_export = CustomSegmentVettedFileUpload.objects.create(segment=segment)
                    vetted_export.download_url = results["download_url"]
                    vetted_export.completed_at = timezone.now()
                    vetted_export.save()
                    segment.save()
                except Exception:
                    logger.exception(f"Error generating vetted segment for id: {segment.id}")
    finally:
        unlock(LOCK_NAME, fail_silently=True)
