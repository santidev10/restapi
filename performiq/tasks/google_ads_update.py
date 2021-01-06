import datetime

from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.tasks.update_campaigns import update_campaigns_task
from saas import celery_app
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock
from utils.datetime import now_in_default_tz

LOCK_PREFIX = "performiq_google_ads_update_"
UPDATE_THRESHOLD = 3600 * 2


@celery_app.task
def google_ads_update_task():
    """
    Main scheduler task to start individual account update tasks
    """
    now = now_in_default_tz()
    update_threshold = now - datetime.timedelta(seconds=UPDATE_THRESHOLD)
    accounts = OAuthAccount.objects\
        .filter(oauth_type=OAuthType.GOOGLE_ADS.value, updated_at__lte=update_threshold)\
        .order_by("updated_at")
    for account in accounts:
        lock, is_acquired = get_lock(account.id)
        if is_acquired:
            update_campaigns_task(account.id)
            account.updated_at = now_in_default_tz()
            account.save()
            unlock.run(lock_name=lock, fail_silently=True)


def get_lock(account_id):
    lock = LOCK_PREFIX + str(account_id)
    is_acquired = REDIS_CLIENT.lock(lock, timeout=60 * 60 * 2).acquire(blocking=False)
    return lock, is_acquired