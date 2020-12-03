import concurrent.futures

from aw_reporting.adwords_api import get_all_customers
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
        mcc_accounts, cid_accounts = get_accounts(oauth_account.refresh_token)
    if mcc_accounts:
        for mcc in mcc_accounts:
            update_mcc_campaigns(mcc["customerId"], oauth_account.refresh_token)
    elif cid_accounts:
        for cid in cid_accounts:
            update_cid_campaigns(cid["customerId"], oauth_account.refresh_token)


def update_mcc_campaigns(mcc_id: int, refresh_token: str):
    """
    Update campaigns for MCC account
    :param mcc_id: Google Ads MCC account id
    :param refresh_token: OAuthAccount.refresh_token
    :return:
    """
    client = get_client(client_customer_id=mcc_id, refresh_token=refresh_token)
    cid_accounts = get_all_customers(client)

    for batch in chunks_generator(cid_accounts, size=20):
        with concurrent.futures.thread.ThreadPoolExecutor(max_workers=20) as executor:
            all_args = [(int(cid["customerId"]), refresh_token) for cid in batch]
            futures = [executor.submit(get_report, *args) for args in all_args]
            reports_data = [f.result() for f in concurrent.futures.as_completed(futures)]
        for account_id, report in reports_data:
            update_create_campaigns(report, account_id)


def update_cid_campaigns(account_id, refresh_token) -> None:
    """
    Update or create campaigns by retrieving report data and creating / updating items for single Account
        Default fields and report query will be used if None given
    :param account_id: Account id
    :param refresh_token: str -> OAuthAccount.refresh_token
    """
    account_id, report = get_report(account_id, refresh_token)
    update_create_campaigns(report, account_id)


def get_report(account_id, refresh_token):
    """ Retrieve Campaign report for Google Ads account id"""
    client = get_client(client_customer_id=account_id, refresh_token=refresh_token)
    fields = [*CAMPAIGN_REPORT_FIELDS_MAPPING.values(), "Clicks", "CampaignStatus"]
    report = get_campaign_report(client, fields, predicates=CAMPAIGN_REPORT_PREDICATES)
    return account_id, report


def update_create_campaigns(report, account_id):
    """ Update or create campaigns from Adwords API Campaign Report """
    to_update, to_create = prepare_items(
        report, Campaign, CAMPAIGN_REPORT_FIELDS_MAPPING, OAuthType.GOOGLE_ADS.value,
        defaults={"account_id": account_id}
    )
    safe_bulk_create(Campaign, to_create)
    update_fields = [val for val in CAMPAIGN_REPORT_FIELDS_MAPPING.keys() if val not in {"id"}]
    Campaign.objects.bulk_update(to_update, fields=update_fields)