import concurrent.futures
import logging

from aw_reporting.adwords_api import get_all_customers
from aw_reporting.adwords_reports import AccountInactiveError
from oauth.models import Campaign
from oauth.models import OAuthAccount
from oauth.utils.client import get_client
from oauth.utils.adwords import clean_update_fields
from oauth.utils.adwords import get_campaign_report
from oauth.utils.adwords import prepare_items
from oauth.utils.adwords import update_accounts
from oauth.tasks.update_ad_groups import update_adgroups_task
from utils.db.functions import safe_bulk_create
from utils.utils import chunks_generator


logger = logging.getLogger(__name__)

MAX_THREADS = 30
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
    {"field": "CampaignStatus", "operator": "IN", "values": ["PAUSED", "ENABLED"]},
]


def update_mcc_campaigns(mcc_id: int, oauth_account: OAuthAccount, cids=None):
    """
    Update campaigns for MCC account
    :param mcc_id: Google Ads MCC account id
    :param oauth_account: OAuthAccount
    :param cids: CID Account ids under MCC
    :return:
    """
    if not cids:
        client = get_client(client_customer_id=mcc_id, refresh_token=oauth_account.refresh_token)
        all_customers = get_all_customers(client)
        cids = [a["customerId"] for a in all_customers]
        update_accounts(oauth_account, all_customers)
    for batch in chunks_generator(cids, size=MAX_THREADS):
        with concurrent.futures.thread.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            all_args = [(cid, oauth_account.refresh_token) for cid in batch]
            futures = [executor.submit(get_report, *args) for args in all_args]
            reports_data = [f.result() for f in concurrent.futures.as_completed(futures)]
        for account_id, report in reports_data:
            # Account is invalid if get_report return None. Do not continue processing
            if report is not None:
                update_create_campaigns(report, account_id, oauth_account.refresh_token)


def update_cid_campaigns(account_id, oauth_account: OAuthAccount) -> None:
    """
    Update or create campaigns by retrieving report data and creating / updating items for single Account
        Default fields and report query will be used if None given
    :param account_id: Account id
    :param oauth_account: str -> OAuthAccount
    """
    account_id, report = get_report(account_id, oauth_account.refresh_token)
    update_create_campaigns(report, account_id, oauth_account.refresh_token)
    oauth_account.gads_accounts.add(account_id)


def update_create_campaigns(report, account_id, refresh_token):
    """
    Update or create campaigns from Adwords API Campaign Report
    Invoke update_adgroups_task once campaign update is complete
    """
    to_update, to_create = prepare_items(
        report, Campaign, CAMPAIGN_REPORT_FIELDS_MAPPING, defaults={"account_id": account_id}
    )
    # Add account id FK if missed in previous updates. Account id is not returned in API
    # response so we must set manually
    for campaign in to_update:
        campaign.account_id = account_id
    safe_bulk_create(Campaign, to_create)
    Campaign.objects.bulk_update(to_update,
                                 fields=["account_id", *clean_update_fields(CAMPAIGN_REPORT_FIELDS_MAPPING.keys())])
    update_adgroups_task(account_id, refresh_token)


def get_report(account_id: int, refresh_token: str):
    """ Retrieve Campaign report for Google Ads account id"""
    try:
        client = get_client(client_customer_id=account_id, refresh_token=refresh_token)
        fields = [*CAMPAIGN_REPORT_FIELDS_MAPPING.values(), "Clicks", "CampaignStatus"]
        report = get_campaign_report(client, fields, predicates=CAMPAIGN_REPORT_PREDICATES)
    except AccountInactiveError:
        report = None
    return account_id, report
