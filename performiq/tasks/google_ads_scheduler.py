import datetime

from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.tasks.constants import Schedulers
from performiq.tasks.update_campaigns import update_campaigns_task
from saas import celery_app
from saas.configs.celery import Queue
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock
from utils.celery.utils import get_queue_size
from utils.datetime import now_in_default_tz

LOCK_PREFIX = "performiq_google_ads_update_"
UPDATE_THRESHOLD = 3600 * 2


@celery_app.task
def google_ads_update_scheduler():
    """
    Main scheduler task to start individual account update tasks
    """
    queue_size = get_queue_size(Queue.PERFORMIQ)
    # limit queue size to prevent queue growing uncontrollably
    limit = Schedulers.GoogleAdsUpdateScheduler.get_items_limit(queue_size)
    accounts = OAuthAccount.objects.filter(oauth_type=OAuthType.GOOGLE_ADS.value).order_by("updated_at")[:limit]
    for account in accounts:
        lock, is_acquired = get_lock(account.id)
        if is_acquired:
            now = now_in_default_tz()
            if account.updated_at < now - datetime.timedelta(seconds=UPDATE_THRESHOLD):
                update_campaigns_task(account.id)
                account.updated_at = now_in_default_tz()
                account.save()
            unlock.run(lock_name=lock, fail_silently=True)


def get_lock(account_id):
    lock = LOCK_PREFIX + str(account_id)
    is_acquired = REDIS_CLIENT.lock(lock, timeout=60 * 60 * 2).acquire(blocking=False)
    return lock, is_acquired
