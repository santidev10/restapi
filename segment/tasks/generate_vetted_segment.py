import logging

from django.utils import timezone

from saas import celery_app
from segment.models import CustomSegment
from segment.models import CustomSegmentVettedFileUpload
from segment.tasks.generate_segment import generate_segment
from segment.api.serializers.custom_segment_vetted_export_serializers import CustomSegmentChannelVettedExportSerializer
from segment.api.serializers.custom_segment_vetted_export_serializers import CustomSegmentVideoVettedExportSerializer

logger = logging.getLogger(__name__)


@celery_app.task
def generate_vetted_segment(segment_id):
    try:
        segment = CustomSegment.objects.get(id=segment_id)
        if segment.segment_type == 0:
            segment.serializer = CustomSegmentVideoVettedExportSerializer
        else:
            segment.serializer = CustomSegmentChannelVettedExportSerializer
        query = segment.get_vetted_items_query()
        s3_key = segment.get_vetted_s3_key()
        results = generate_segment(segment, query, segment.LIST_SIZE, add_uuid=False, s3_key=s3_key, vetted=True)
        if hasattr(segment, "vetted_export"):
            segment.vetted_export.delete()
        vetted_export = CustomSegmentVettedFileUpload.objects.create(segment=segment)
        vetted_export.download_url = results["download_url"]
        vetted_export.completed_at = timezone.now()
        vetted_export.save()
        segment.save()
    except CustomSegment.DoesNotExist:
        logger.error(f"Segment with id: {segment_id} does not exist.")
    except Exception:
        logger.exception(f"Error generating vetted segment for id: {segment_id}")
