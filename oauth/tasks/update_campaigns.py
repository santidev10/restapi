import concurrent.futures
import logging

from googleads.errors import GoogleAdsServerFault
from google.auth.exceptions import RefreshError

from aw_reporting.adwords_api import get_all_customers
from oauth.models import Account
from oauth.models import Campaign
from oauth.models import OAuthAccount
from oauth.utils.client import get_client
from oauth.utils.adwords import clean_update_fields
from oauth.utils.adwords import get_campaign_report
from oauth.utils.adwords import get_accounts
from oauth.utils.adwords import prepare_items
from oauth.tasks.update_ad_groups import update_adgroups_task
from saas import celery_app
from utils.db.functions import safe_bulk_create
from utils.utils import chunks_generator


logger = logging.getLogger(__name__)

GADS_CID_UPDATE_THRESHOLD = 3600 * 2
CAMPAIGN_REPORT_FIELDS_MAPPING = dict(
    id="CampaignId",
    impressions="Impressions",
    video_views="VideoViews",
    cost="Cost",
    name="CampaignName",
    active_view_viewability="ActiveViewViewability",
    video_quartile_100_rate="VideoQuartile100Rate",
    ctr="Ctr",
    cpm="AverageCpm",
    cpv="AverageCpv",
)

CAMPAIGN_REPORT_PREDICATES = [
    {"field": "ServingStatus", "operator": "EQUALS", "values": ["SERVING"]},
    {"field": "CampaignStatus", "operator": "EQUALS", "values": ["ENABLED"]},
]


@celery_app.task
def update_campaigns_task(oauth_account_id: int, mcc_accounts=None, cid_accounts=None):
    """
    Updates campaigns depending if OAuthAccount is MCC or regular cid
    :param oauth_account_id:
    :param mcc_accounts: list -> Adwords API CustomerService response
    :param cid_accounts: list -> Adwords API CustomerService response
    :return:
    """
    oauth_account = OAuthAccount.objects.get(id=oauth_account_id)
    if mcc_accounts is None and cid_accounts is None:
        try:
            mcc_accounts, cid_accounts = get_accounts(oauth_account.refresh_token)
        except (GoogleAdsServerFault, RefreshError):
            logger.warning(f"Error updating campaigns for OAuthAccount id: {oauth_account_id}")
            return
        except Exception:
            logger.exception(f"Unexpected Exception updating campaigns for OAuthAccount id: {oauth_account_id}")
            return

    if mcc_accounts:
        for mcc in mcc_accounts:
            update_mcc_campaigns(mcc["customerId"], oauth_account)
    elif cid_accounts:
        for cid in cid_accounts:
            update_cid_campaigns(cid["customerId"], oauth_account)

    for account in (mcc_accounts or []) + (cid_accounts or []):
        update_adgroups_task(account["customerId"], oauth_account.refresh_token)

    oauth_account.synced = True
    oauth_account.save(update_fields=["synced"])


def update_mcc_campaigns(mcc_id: int, oauth_account: OAuthAccount):
    """
    Update campaigns for MCC account
    :param mcc_id: Google Ads MCC account id
    :param oauth_account: OAuthAccount
    :return:
    """
    client = get_client(client_customer_id=mcc_id, refresh_token=oauth_account.refresh_token)
    all_cids = [int(cid["customerId"]) for cid in get_all_customers(client)]
    existing = oauth_account.gads_accounts.values_list("id", flat=True)

    for batch in chunks_generator(all_cids, size=20):
        with concurrent.futures.thread.ThreadPoolExecutor(max_workers=20) as executor:
            all_args = [(cid, oauth_account.refresh_token) for cid in batch]
            futures = [executor.submit(get_report, *args) for args in all_args]
            reports_data = [f.result() for f in concurrent.futures.as_completed(futures)]
        for account_id, report in reports_data:
            update_create_campaigns(report, account_id)

    oauth_account.gads_accounts.add(*set(all_cids) - set(existing))


def update_cid_campaigns(account_id, oauth_account: OAuthAccount) -> None:
    """
    Update or create campaigns by retrieving report data and creating / updating items for single Account
        Default fields and report query will be used if None given
    :param account_id: Account id
    :param oauth_account: str -> OAuthAccount
    """
    account_id, report = get_report(account_id, oauth_account.refresh_token)
    update_create_campaigns(report, account_id)
    oauth_account.gads_accounts.add(account_id)


def get_report(account_id: int, refresh_token: str):
    """ Retrieve Campaign report for Google Ads account id"""
    client = get_client(client_customer_id=account_id, refresh_token=refresh_token)
    fields = [*CAMPAIGN_REPORT_FIELDS_MAPPING.values(), "Clicks", "CampaignStatus"]
    report = get_campaign_report(client, fields, predicates=CAMPAIGN_REPORT_PREDICATES)
    return account_id, report


def update_create_campaigns(report, account_id):
    """ Update or create campaigns from Adwords API Campaign Report """
    account, _ = Account.objects.get_or_create(id=account_id)
    to_update, to_create = prepare_items(
        report, Campaign, CAMPAIGN_REPORT_FIELDS_MAPPING, defaults={"account_id": account.id}
    )
    safe_bulk_create(Campaign, to_create)
    Campaign.objects.bulk_update(to_update, fields=clean_update_fields(CAMPAIGN_REPORT_FIELDS_MAPPING.keys()))
