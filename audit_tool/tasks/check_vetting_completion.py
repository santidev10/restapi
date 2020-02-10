from django.utils import timezone

from audit_tool.models import AuditProcessor
from audit_tool.models import AuditChannelVet
from utils.celery.tasks import REDIS_CLIENT


LOCK_NAME = "audit_tool.check_vetting_completion"


def check_vetting_completion():
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, timeout=60).acquire(blocking=False)
    """
    Check if all items in audit have been vetted to set completed_at values
    """
    if is_acquired:
        incomplete = AuditProcessor.objects.filter(completed=None, source=1)
        for audit in incomplete:
            if audit.audit_type == 1:
                # Tech debt 4.8 - Add AuditVideoVet models
                vetting_model = None
            elif audit.audit_type == 2:
                vetting_model = AuditChannelVet
            still_processing = vetting_model.objects.filter(audit=audit, processed=None)
            if not still_processing:
                audit.completed_at = timezone.now()
                audit.save()
