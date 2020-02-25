from django.utils import timezone

from audit_tool.models import AuditProcessor
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditVideoVet
from saas import celery_app
from segment.models import CustomSegment
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock


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
    incomplete_segments = CustomSegment.objects.filter(audit_id__isnull=False, is_vetting_complete=False)
    incomplete_audits = AuditProcessor.objects.filter(id__in=incomplete_segments.values_list("audit_id", flat=True))

    incomplete = []
    complete = []
    for audit in incomplete_audits:
        if audit.audit_type == 1:
            vetting_model = AuditVideoVet
        elif audit.audit_type == 2:
            vetting_model = AuditChannelVet
        else:
            raise ValueError(
                f"Audit id: {audit.id} with incompatible audit_type: {audit.audit_type} with source: {audit.source}")
        still_processing = vetting_model.objects.filter(audit=audit, processed=None).exists()
        # Must still check to set audits completed_at to None if admin flags vetting items
        if still_processing:
            incomplete.append(audit.id)
        else:
            complete.append(audit.id)
            audit.completed = timezone.now()
            audit.save()
    CustomSegment.objects.filter(audit_id__in=complete).update(is_vetting_complete=True)
    CustomSegment.objects.filter(audit_id__in=incomplete).update(is_vetting_complete=False)
