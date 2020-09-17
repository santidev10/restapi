from datetime import timedelta

from aw_reporting.adwords_api import get_all_customers
from performiq.models import Campaign
from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.utils.constants import CAMPAIGN_FIELDS_MAPPING
# from performiq.utils.adwords_report import get_client
from performiq.utils.adwords_report import get_campaign_report
from performiq.utils.adwords_report import get_accounts
from performiq.utils.update import prepare_items
from performiq.oauth_utils import get_client
from saas import celery_app
from utils.datetime import now_in_default_tz
from utils.db.functions import safe_bulk_create


FROM_HISTORICAL_DAYS = 90


def update_campaigns_task(oauth_account_id, mcc_accounts=None, cid_accounts=None):
    oauth_account = OAuthAccount.objects.get(id=oauth_account_id)
    if mcc_accounts is None and cid_accounts is None:
        mcc_accounts, cid_accounts = get_accounts(oauth_account.refresh_token)
    if mcc_accounts:
        first = mcc_accounts[0]
        update_mcc_campaigns_task(first["customerId"], ouath_account_id=oauth_account.id)
    elif cid_accounts:
        for cid in cid_accounts:
            update_cid_campaigns_task(cid["customerId"], ouath_account_id=oauth_account.id)


@celery_app.task
def update_mcc_campaigns_task(client_customer_id, ouath_account_id=None):
    client = get_client(client_customer_id, ouath_account_id)
    cids = get_all_customers(client)[-10:]
    for cid in cids:
        update_cid_campaigns_task(cid["customerId"], ouath_account_id=ouath_account_id)


@celery_app.task
def update_cid_campaigns_task(account_id, historical=False, ouath_account_id=None):
    date_range = None
    predicates = None
    if historical is True:
        today = now_in_default_tz().date()
        from_date = (today - timedelta(days=FROM_HISTORICAL_DAYS)).strftime("%Y%m%d")
        predicates = [{"field": "EndDate", "operator": "GREATER_THAN_EQUALS", "values": [from_date]}]
    update_campaigns(account_id, date_range=date_range, predicates=predicates, ouath_account_id=ouath_account_id)


def update_campaigns(account_id: int, predicates=None, date_range=None, ouath_account=None) -> None:
    """
    Update campaigns by retrieving report data and creating / updating items
        Default fields and report query will be used if None given
    :param account_id: int -> Google Ads CID account id
    :param predicates: dict -> Adwords reports predicates selector
    """
    client = get_client(client_customer_id=account_id, ouath_account_id=ouath_account_id)
    report = get_campaign_report(client, predicates=predicates, date_range=date_range, addl_fields=["Clicks"])
    to_update, to_create = prepare_items(
        report, Campaign, CAMPAIGN_FIELDS_MAPPING, OAuthType.GOOGLE_ADS.value, ["cpm", "cpv", "ctr"]
    )
    if ouath_account_id is None:
        ouath_account_fk = account_id
    else:
        ouath_account_fk = ouath_account_id
    for item in to_update + to_create:
        item.account_id = ouath_account_fk
    safe_bulk_create(Campaign, to_create)
    update_fields = [val for val in CAMPAIGN_FIELDS_MAPPING.keys() if val not in {"id"}]
    Campaign.objects.bulk_update(to_update, fields=update_fields)


# def update_campaigns(account_id: int, predicates=None, date_range=None, ouath_account=None) -> None:
#     """
#     Update campaigns by retrieving report data and creating / updating items
#         Default fields and report query will be used if None given
#     :param account_id: int -> Google Ads CID account id
#     :param predicates: dict -> Adwords reports predicates selector
#     """
#     client = get_client(client_customer_id=account_id, ouath_account_id=ouath_account_id)
#     report = get_campaign_report(client, predicates=predicates, date_range=date_range, addl_fields=["Clicks"])
#     to_update, to_create = prepare_items(
#         report, Campaign, CAMPAIGN_FIELDS_MAPPING, OAuthType.GOOGLE_ADS.value, ["cpm", "cpv", "ctr"]
#     )
#     if ouath_account_id is None:
#         ouath_account_fk = account_id
#     else:
#         ouath_account_fk = ouath_account_id
#     for item in to_update + to_create:
#         item.account_id = ouath_account_fk
#     safe_bulk_create(Campaign, to_create)
#     update_fields = [val for val in CAMPAIGN_FIELDS_MAPPING.keys() if val not in {"id"}]
#     Campaign.objects.bulk_update(to_update, fields=update_fields)
