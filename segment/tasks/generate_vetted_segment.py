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
    :param recipient: str -> Email of export. Used for vetting exports still in progreess
    If false, generate export and email user
    :return:
    """
    try:
        segment = CustomSegment.objects.get(id=segment_id)
        segment.set_vetting()
        query = segment.get_vetted_items_query()
        s3_key = segment.get_vetted_s3_key(suffix=str(timezone.now()))
        results = generate_segment(segment, query, segment.LIST_SIZE, add_uuid=False, s3_key=s3_key, vetted=True)
        if recipient:
            send_export_email(recipient, segment.title, results["download_url"])
        else:
            # Do not save export in progress since progress may rapidly change
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
