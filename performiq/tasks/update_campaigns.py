import concurrent.futures
import datetime
import logging
from typing import List

from googleads.errors import GoogleAdsServerFault

from aw_reporting.adwords_api import get_all_customers
from performiq.models import Account
from performiq.models import Campaign
from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.utils.adwords_report import get_campaign_report
from performiq.utils.adwords_report import get_accounts
from performiq.utils.update import prepare_items
from performiq.oauth_utils import get_client
from saas import celery_app
from utils.db.functions import safe_bulk_create
from utils.utils import chunks_generator
from utils.datetime import now_in_default_tz


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
        except GoogleAdsServerFault:
            logger.exception(f"Error updating campaigns for OAuthAccount id: {oauth_account_id}")
            return

    if mcc_accounts:
        for mcc in mcc_accounts:
            update_mcc_campaigns(mcc["customerId"], oauth_account.refresh_token)
    elif cid_accounts:
        for cid in cid_accounts:
            update_cid_campaigns(cid["customerId"], oauth_account.refresh_token)

    oauth_account.synced = True
    oauth_account.save(update_fields=["synced"])


def _get_cids_to_update(all_cids: List[int]) -> List[int]:
    """
    Get CID's to update based on updated_at
    Some Oauth accounts will retrieve the same Google Ads CID's as they may have shared access to MCC accounts.
    To prevent constant updating of same Google Ads CID's from different OAuthAccounts, exclude recently
    updated accounts
    :param all_cids: CID's retrieved with get_all_customers
    :return:
    """
    recently = now_in_default_tz() - datetime.timedelta(minutes=GADS_CID_UPDATE_THRESHOLD)
    recently_updated = Account.objects \
        .filter(id__in=all_cids, updated_at__gte=recently) \
        .values_list("id", flat=True)
    to_update_cids = list(set(all_cids) - set(recently_updated))
    return to_update_cids


def update_mcc_campaigns(mcc_id: int, refresh_token: str):
    """
    Update campaigns for MCC account
    :param mcc_id: Google Ads MCC account id
    :param refresh_token: OAuthAccount.refresh_token
    :return:
    """
    client = get_client(client_customer_id=mcc_id, refresh_token=refresh_token)
    all_cids = [int(cid["customerId"]) for cid in get_all_customers(client)]
    to_update_cids = _get_cids_to_update(all_cids)

    for batch in chunks_generator(to_update_cids, size=20):
        with concurrent.futures.thread.ThreadPoolExecutor(max_workers=20) as executor:
            all_args = [(cid, refresh_token) for cid in batch]
            futures = [executor.submit(get_report, *args) for args in all_args]
            reports_data = [f.result() for f in concurrent.futures.as_completed(futures)]
        for account_id, report in reports_data:
            update_create_campaigns(report, account_id)


def update_cid_campaigns(account_id: int, refresh_token: str) -> None:
    """
    Update or create campaigns by retrieving report data and creating / updating items for single Account
        Default fields and report query will be used if None given
    :param account_id: Account id
    :param refresh_token: str -> OAuthAccount.refresh_token
    """
    account_id, report = get_report(account_id, refresh_token)
    update_create_campaigns(report, account_id)


def get_report(account_id: int, refresh_token: str):
    """ Retrieve Campaign report for Google Ads account id"""
    client = get_client(client_customer_id=account_id, refresh_token=refresh_token)
    fields = [*CAMPAIGN_REPORT_FIELDS_MAPPING.values(), "Clicks", "CampaignStatus"]
    report = get_campaign_report(client, fields, predicates=CAMPAIGN_REPORT_PREDICATES)
    return account_id, report


def update_create_campaigns(report, account_id: int):
    """ Update or create campaigns from Adwords API Campaign Report """
    to_update, to_create = prepare_items(
        report, Campaign, CAMPAIGN_REPORT_FIELDS_MAPPING, OAuthType.GOOGLE_ADS.value,
        defaults={"account_id": account_id}
    )
    safe_bulk_create(Campaign, to_create)
    update_fields = [val for val in CAMPAIGN_REPORT_FIELDS_MAPPING.keys() if val not in {"id"}]
    Campaign.objects.bulk_update(to_update, fields=update_fields)