import logging

from django.db.models import F

from saas import celery_app
from segment.utils import get_segment_model_by_type
from segment.utils import update_all_segments_statistics
from segment.models.keyword import SegmentKeyword
from segment.models.video import SegmentVideo
from segment.models.channel import SegmentChannel


logger = logging.getLogger(__name__)


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


@celery_app.task
def cleanup_channel_segments_related_records():
    SegmentChannel.objects.cleanup_related_records()


@celery_app.task
def cleanup_video_segments_related_records():
    SegmentVideo.objects.cleanup_related_records()


@celery_app.task
def cleanup_keyword_segments_related_records():
    SegmentKeyword.objects.cleanup_related_records()
