import logging

from django.utils import timezone

from audit_tool.models import AuditProcessor
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditVideoVet
from saas import celery_app
from segment.models import CustomSegment
from segment.tasks.generate_vetted_segment import generate_vetted_segment
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock

logger = logging.getLogger(__name__)


LOCK_NAME = "audit_tool.check_vetting_completion"


@celery_app.task(expires=60 * 5, soft_time_limit=60 * 5)
def check_vetting_completion_task():
    """
    Check if all items in audit have been vetted to set completed_at values
    """
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, timeout=60 * 10).acquire(blocking=False)
    if is_acquired:
        check_vetting_completion()
        unlock.run(lock_name=LOCK_NAME, fail_silently=True)


def check_vetting_completion():
    """
    Check segment vetting completion
    If was completed and now incomplete, delete vetted export
    If newly completed, execute generate_vetted_segment task for segment
    :return:
    """
    segment_audits = {
        item.audit_id: item
        for item in CustomSegment.objects.filter(audit_id__isnull=False, is_vetting_complete=False)
    }
    audits = AuditProcessor.objects.filter(id__in=list(segment_audits.keys()))
    for audit in audits:
        segment = segment_audits.get(audit.id)
        if segment is None:
            logger.error(f"Audit with missing segment. audit_id: {audit.id}")
            continue
        if audit.audit_type == 1:
            vetting_model = AuditVideoVet
        elif audit.audit_type == 2:
            vetting_model = AuditChannelVet
        else:
            raise ValueError(
                f"Audit id: {audit.id} with incompatible audit_type: {audit.audit_type} with source: {audit.source}")
        still_processing = vetting_model.objects.filter(audit=audit, processed=None).exists()
        # Create permanent export for completed vetted segment
        if not still_processing:
            generate_vetted_segment.delay(segment.id)
            segment.is_vetting_complete = True
            audit.completed = timezone.now()
            audit.save()
            segment.save()
