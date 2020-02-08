from datetime import timedelta

from django.utils import timezone

from audit_tool.models import AuditChannelVet
from utils.celery.tasks import REDIS_CLIENT

# 10 minutes
CHECKOUT_THRESHOLD = 10
LOCK_NAME = "audit_tool.check_in_vetting_items"


def check_in_vetting_items():
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, timeout=60 * 5).acquire(blocking=False)
    """
    Set vetting items is_checked_out values to False if checked out beyond threshold
    Prevents items from being checked out
    """
    if is_acquired:
        threshold = timezone.now() - timedelta(minutes=CHECKOUT_THRESHOLD)
        AuditChannelVet.objects.filter(checked_out_at__lt=threshold).update(is_checked_out=False)
