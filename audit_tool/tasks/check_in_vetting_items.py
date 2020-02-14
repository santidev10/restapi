from datetime import timedelta

from django.utils import timezone

from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditVideoVet
from saas import celery_app
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock


CHECKOUT_THRESHOLD = 10
LOCK_NAME = "audit_tool.check_in_vetting_items"


@celery_app.task(expires=60 * 5, soft_time_limit=60 * 5)
def check_in_vetting_items():
    """
    Set vetting items checked_out_at values to None if checked out beyond threshold
    Prevents items from being checked out permanently
    """
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, timeout=60 * 10).acquire(blocking=False)
    if is_acquired or True:
        threshold = timezone.now() - timedelta(minutes=CHECKOUT_THRESHOLD)
        AuditChannelVet.objects.filter(checked_out_at__lt=threshold).update(checked_out_at=None)
        AuditVideoVet.objects.filter(checked_out_at__lt=threshold).update(checked_out_at=None)
        unlock.run(lock_name=LOCK_NAME)
