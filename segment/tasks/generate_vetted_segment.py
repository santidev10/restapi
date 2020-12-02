import logging

from django.conf import settings
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
    If recipient, then user requests vetted export in progress
    Else, vetting is complete. Create CustomSegmentVettedFileUpload and notify admins
    :return:
    """
    # pylint: disable=broad-except
    try:
        segment = CustomSegment.objects.get(id=segment_id)
        segment.is_vetting = True
        query = segment.get_vetted_items_query()
        # If recipient, user requested export of vetting in progress. Generate temp export as vetting progress
        # may rapidly change
        s3_key_suffix = str(timezone.now()) if recipient else None
        s3_key = segment.get_vetted_s3_key(suffix=s3_key_suffix)
        if segment.owner.is_staff or segment.owner.has_perm("userprofile.vet_audit_admin"):
            size = segment.config.ADMIN_LIST_SIZE
        else:
            size = segment.config.USER_LIST_SIZE
        results = generate_segment(segment, query, size, add_uuid=False, s3_key=s3_key)
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
            vetted_export.filename = s3_key
            vetted_export.save()
            segment.save()
            send_export_email(settings.VETTING_EXPORT_EMAIL_RECIPIENTS, segment.title, results["download_url"])
    except CustomSegment.DoesNotExist:
        logger.error("Segment with id: % does not exist.", segment_id)
    except Exception:
        logger.exception("Error generating vetted segment for id: %s", segment_id)
    # pylint: enable=broad-except
