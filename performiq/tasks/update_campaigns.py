from datetime import timedelta

from aw_reporting.adwords_api import get_all_customers
from performiq.models import Campaign
from performiq.models import OAuthAccount
from performiq.models import Account
from performiq.models.constants import OAuthType
from performiq.utils.constants import CAMPAIGN_FIELDS_MAPPING
from performiq.utils.adwords_report import get_campaign_report
from performiq.utils.adwords_report import get_accounts
from performiq.utils.update import prepare_items
from performiq.oauth_utils import get_client
from saas import celery_app
from utils.datetime import now_in_default_tz
from utils.db.functions import safe_bulk_create


FROM_HISTORICAL_DAYS = 90

# @celery_app.task
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
        first = mcc_accounts[0]
        update_mcc_campaigns_task(first["customerId"], oauth_account_id=oauth_account.id)
    elif cid_accounts:
        for cid in cid_accounts:
            update_cid_campaigns_task(cid["customerId"], oauth_account_id=oauth_account.id)


@celery_app.task
def update_mcc_campaigns_task(mcc_id: int, oauth_account_id: int):
    """
    Update campaigns for MCC account
    :param mcc_id: Google Ads MCC account id
    :param oauth_account_id: OAuthAccount pk
    :return:
    """
    ouath_account = OAuthAccount.objects.get(id=oauth_account_id)
    client = get_client(client_customer_id=mcc_id, refresh_token=ouath_account.refresh_token)
    cids = get_all_customers(client)[-10:]
    for cid_account_data in cids:
        cid_account_id = int(cid_account_data["customerId"])
        cid_account, _ = Account.objects.get_or_create(id=cid_account_id, oauth_account_id=oauth_account_id)
        update_cid_campaigns_task(cid_account_id)


@celery_app.task
def update_cid_campaigns_task(account_id: int, historical=False):
    """
    Update single Google Ads CID account
    :param account_id: Account model id, which is a Google Ads CID account id
    :param historical: bool -> Determines if we will pull historical campaigns using end date
    :return:
    """
    account = Account.objects.get(id=account_id)
    date_range = None
    predicates = None
    if historical is True:
        today = now_in_default_tz().date()
        from_date = (today - timedelta(days=FROM_HISTORICAL_DAYS)).strftime("%Y%m%d")
        predicates = [{"field": "EndDate", "operator": "GREATER_THAN_EQUALS", "values": [from_date]}]
    update_campaigns(account, date_range=date_range, predicates=predicates)


def update_campaigns(account: Account, predicates=None, date_range=None) -> None:
    """
    Update campaigns by retrieving report data and creating / updating items
        Default fields and report query will be used if None given
    :param google_ads_cid: int -> Google Ads CID account id
    :param ouath_account: OAuthAccount
    :param predicates: dict -> Adwords reports predicates selector
    :param date_range: dict -> {"min": date_obj, "max": date_obj}
    :param
    """
    client = get_client(client_customer_id=account.id, refresh_token=account.oauth_account.refresh_token)
    report = get_campaign_report(client, predicates=predicates, date_range=date_range, addl_fields=["Clicks"])
    to_update, to_create = prepare_items(
        report, Campaign, CAMPAIGN_FIELDS_MAPPING, OAuthType.GOOGLE_ADS.value, ["cpm", "cpv", "ctr"]
    )
    # Set FK Account id
    for item in to_update + to_create:
        item.account_id = account.id
    safe_bulk_create(Campaign, to_create)
    update_fields = [val for val in CAMPAIGN_FIELDS_MAPPING.keys() if val not in {"id"}]
    Campaign.objects.bulk_update(to_update, fields=update_fields)
