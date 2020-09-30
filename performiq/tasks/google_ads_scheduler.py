from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.tasks.update_campaigns import update_campaigns_task
from saas import celery_app
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock

LOCK_PREFIX = "performiq_google_ads_update_"


@celery_app.task
def google_ads_update_scheduler():
    accounts = OAuthAccount.objects.filter(oauth_type=OAuthType.GOOGLE_ADS.value)
    for account in accounts:
        lock, is_acquired = get_lock(account.id)
        if is_acquired:
            update_campaigns_task.delay(account.id)
            unlock.run(lock_name=lock, fail_silently=True)

def get_lock(account_id):
    lock = LOCK_PREFIX + str(account_id)
    is_acquired = REDIS_CLIENT.lock(lock, timeout=60 * 60 * 2).acquire(blocking=False)
    return lock, is_acquired