import logging

from django.utils import timezone

from saas import celery_app
from segment.models import CustomSegment
from segment.models import CustomSegmentVettedFileUpload
from segment.tasks.generate_segment import generate_segment
from segment.utils.send_export_email import send_export_email

logger = logging.getLogger(__name__)


@celery_app.task
def generate_vetted_segment(segment_id, recipient=None):
    """
    Generate vetted custom segment export
    :param segment_id: int
    :param recipient: str -> Email of export. Used for vetting exports still in progress
    If false, generate export and email user
    :return:
    """
    try:
        segment = CustomSegment.objects.get(id=segment_id)
        segment.set_vetting()
        query = segment.get_vetted_items_query()
        # If recipient, vetting is still in progress. Generate temp export as progress of vetting may rapidly change
        s3_key_suffix = str(timezone.now()) if recipient else None
        s3_key = segment.get_vetted_s3_key(suffix=s3_key_suffix)
        results = generate_segment(segment, query, segment.LIST_SIZE, add_uuid=False, s3_key=s3_key, vetted=True)
        if recipient:
            send_export_email(recipient, segment.title, results["download_url"])
        else:
            try:
                segment.vetted_export.delete()
            except CustomSegmentVettedFileUpload.DoesNotExist:
                pass
            vetted_export = CustomSegmentVettedFileUpload.objects.create(segment=segment)
            vetted_export.download_url = results["download_url"]
            vetted_export.completed_at = timezone.now()
            vetted_export.save()
            segment.save()
    except CustomSegment.DoesNotExist:
        logger.error(f"Segment with id: {segment_id} does not exist.")
    except Exception:
        logger.exception(f"Error generating vetted segment for id: {segment_id}")
