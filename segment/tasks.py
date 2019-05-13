import logging

from django.db.models import F

from saas import celery_app
from segment.utils import get_segment_model_by_type
from segment.utils import update_all_segments_statistics
from segment.models.keyword import SegmentKeyword
from segment.models.video import SegmentVideo
from segment.models.channel import SegmentChannel
from utils.celery.tasks import celery_lock


logger = logging.getLogger(__name__)

LOCK_SEGMENTS_CHANNEL_CLEANUP = "cleanup_channel_segments"
LOCK_SEGMENTS_VIDEO_CLEANUP = "cleanup_video_segments"
LOCK_SEGMENTS_KEYWORD_CLEANUP = "cleanup_keyword_segments"
CLEANUP_LOCK_EXPIRE_TIME = 60 * 60 * 8  # 8 hours



@celery_app.task
def fill_segment_from_filters(segment_type, segment_id, filters):
    model = get_segment_model_by_type(segment_type)
    segment = model.objects.get(pk=segment_id)
    segment.add_by_filters(filters)
    model.objects.filter(pk=segment_id).update(pending_updates=F("pending_updates") - 1)


@celery_app.task
def update_segments_stats():
    update_all_segments_statistics()


@celery_app.task
def cleanup_segments_related():
    cleanup_video_segments_related_records.delay()
    cleanup_channel_segments_related_records.delay()
    cleanup_keyword_segments_related_records.delay()


@celery_app.task(bind=True)
@celery_lock(lock_key=LOCK_SEGMENTS_CHANNEL_CLEANUP, expire=CLEANUP_LOCK_EXPIRE_TIME)
def cleanup_channel_segments_related_records(*args):
    logger.info("Segments Channel cleanup_related_records start")
    SegmentChannel.objects.cleanup_related_records()
    logger.info("Segments Channel cleanup_related_records stop")


@celery_app.task(bind=True)
@celery_lock(lock_key=LOCK_SEGMENTS_VIDEO_CLEANUP, expire=CLEANUP_LOCK_EXPIRE_TIME)
def cleanup_video_segments_related_records(*args):
    logger.info("Segments Video cleanup_related_records start")
    SegmentVideo.objects.cleanup_related_records()
    logger.info("Segments Video cleanup_related_records stop")


@celery_app.task(bind=True)
@celery_lock(lock_key=LOCK_SEGMENTS_KEYWORD_CLEANUP, expire=CLEANUP_LOCK_EXPIRE_TIME)
def cleanup_keyword_segments_related_records(*args):
    logger.info("Segments Keyword cleanup_related_records start")
    SegmentKeyword.objects.cleanup_related_records()
    logger.info("Segments Keyword cleanup_related_records stop")
