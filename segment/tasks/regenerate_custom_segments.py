import logging
from datetime import timedelta

from django.conf import settings
from django.db.models import Q
from django.utils import timezone

from saas import celery_app
from segment.models import CustomSegment
from segment.models.constants import VideoExclusion
from segment.tasks.generate_segment import generate_segment
from segment.tasks.generate_video_exclusion import generate_video_exclusion
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock

LOCK_NAME = "regenerate_custom_segments"
EXPIRE = 60 * 30

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
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            logger.exception("Error in regenerate_custom_segments_with_lock task")
        finally:
            unlock(LOCK_NAME, fail_silently=True)


def regenerate_custom_segments():
    """
    regenerate export for segments flagged as `is_regenerating` and last
    updated at least CUSTOM_SEGMENT_REGENERATION_DAYS_THRESHOLD ago. These
    custom segments are the new persistent segments. Typically, an is_featured
    segment is also an is_regenerating segment, but we're separating the
    functionality just in case. These are the new Brand Safety Target Lists.
    All non-featured segments are Custom Target Lists.
    """
    date_threshold = timezone.now() - timedelta(days=settings.CUSTOM_SEGMENT_REGENERATION_DAYS_THRESHOLD)
    query = CustomSegment.objects.filter(Q(is_regenerating=True) & Q(updated_at__lte=date_threshold))
    for segment in query:
        logger.debug("Processing regenerating segment titled: %s", segment.title)
        export = segment.export
        size = segment.config.ADMIN_LIST_SIZE
        results = generate_segment(segment, export.query["body"], size, add_uuid=False)
        segment.statistics = {
            **(segment.statistics or {}),
            **results["statistics"]
        }
        segment.save()
        export.download_url = results["download_url"]
        export.completed_at = timezone.now()
        if export.admin_filename is None:
            export.admin_filename = results["admin_s3_key"]
        export.save()

        if segment.params.get(VideoExclusion.WITH_VIDEO_EXCLUSION) is True:
            generate_video_exclusion(segment)
