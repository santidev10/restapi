import datetime
import logging

from django.db.models import Q
from googleads.errors import GoogleAdsServerFault
from google.auth.exceptions import RefreshError

from .update_campaigns import update_mcc_campaigns
from .update_campaigns import update_cid_campaigns
from aw_reporting.adwords_api import get_all_customers
from oauth.constants import OAuthType
from oauth.models import Account
from oauth.models import OAuthAccount
from oauth.utils.adwords import get_accounts
from oauth.utils.client import get_client
from oauth.utils.adwords import update_accounts
from saas import celery_app
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock
from utils.datetime import now_in_default_tz

LOCK_PREFIX = "oauth_google_ads_update_"
UPDATE_THRESHOLD = 3600 * 2
logger = logging.getLogger(__name__)


@celery_app.task
def google_ads_update_task(oauth_account_ids=None):
    """
    Main scheduler task to start individual account update tasks
    Update process involves filtering for non existent and outdated accounts
    Locks are acquired for each account update to prevent concurrent updates on similar
    resources
    :param oauth_account_ids: list -> OAuthAccount ids to update
    """
    oauth_filter = Q() if oauth_account_ids is None else Q(id__in=oauth_account_ids)
    oauth_accounts = OAuthAccount.objects \
        .filter(oauth_filter, oauth_type=OAuthType.GOOGLE_ADS.value) \
        .order_by("updated_at")
    update_threshold = now_in_default_tz() - datetime.timedelta(seconds=UPDATE_THRESHOLD)
    for oauth in oauth_accounts:
        try:
            mcc_accounts, cid_accounts = get_accounts(oauth.refresh_token)
            update_accounts(oauth, mcc_accounts + cid_accounts, name_field="descriptiveName")
        except (GoogleAdsServerFault, RefreshError):
            logger.warning(f"Error google_ads_update_task for OAuthAccount id: {oauth.id}")
            continue

        for mcc in mcc_accounts:
            # Add relation between current OAuthAccount and existing customer accounts under mcc
            client = get_client(client_customer_id=mcc["customerId"], refresh_token=oauth.refresh_token)
            all_customers = get_all_customers(client)
            to_update_ids = get_to_update(all_customers, update_threshold)
            update_accounts(oauth, all_customers, name_field="name")
            update_with_lock(update_mcc_campaigns, mcc["customerId"], oauth, cids=to_update_ids)

        for cid_id in get_to_update(cid_accounts, update_threshold):
            update_with_lock(update_cid_campaigns, cid_id, oauth)
            oauth.gads_accounts.add(cid_id)

        oauth.synced = True
        oauth.save(update_fields=["synced"])


def get_to_update(accounts: list, update_threshold: datetime.datetime) -> list:
    """
    Retrieve account ids to update that have not been updated within threshold.
    :param accounts: list -> API response of get_accounts function
    :param update_threshold: datetime -> Lowest acceptable update time to update accounts again
    :return: list -> Account ids to retrieve OAuth data for
    """
    ids = [a["customerId"] for a in accounts]
    exists = Account.objects.filter(id__in=ids).values_list("id", flat=True)
    to_update = set(exists.filter(updated_at__lte=update_threshold))
    to_create = set(ids) - set(exists)
    return [*to_create, *to_update]


def update_with_lock(update_func, account_id: int, oauth: OAuthAccount, **kwargs) -> None:
    """
    Invoke update_func for target account id only if lock is acquired
    There may be instances in which many users have access to the same MCC, and if oauthing at
    similar times, updates will be inefficient as the same resources will be retrieved
    :param update_func: Function to perform update
    :param account_id: int
    :param oauth: OAuthAccount
    """
    lock = LOCK_PREFIX + str(account_id)
    is_acquired = REDIS_CLIENT.lock(lock, timeout=3600 * 2).acquire(blocking=False)
    if is_acquired:
        try:
            update_func(account_id, oauth, **kwargs)
            Account.objects.filter(id=account_id).update(updated_at=now_in_default_tz())
        finally:
            unlock(lock_name=lock, fail_silently=True)
