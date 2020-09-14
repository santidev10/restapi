from aw_reporting.adwords_reports import _get_report
from aw_reporting.adwords_reports import _output_to_rows
from performiq.models import Campaign
from performiq.models.constants import OAuthType
from performiq.utils.constants import CAMPAIGN_FIELDS_MAPPING
from performiq.utils.adwords_report import get_client
from performiq.utils.update import prepare_items
from utils.db.functions import safe_bulk_create


def update_campaigns(account_id: int, report_predicates=None) -> None:
    """
    Update campaigns by retrieving report data and creating / updating items
        Default fields and report query will be used if None given
    :param account_id: int -> Google Ads CID account id
    :param report_predicates: dict -> Adwords reports predicates selector
    """
    client = get_client(account_id)
    report = get_campaign_report(client, predicates=report_predicates, addl_fields=["Clicks"])
    to_update, to_create = prepare_items(
        report, Campaign, CAMPAIGN_FIELDS_MAPPING, OAuthType.GOOGLE_ADS.value, ["cpm", "cpv", "ctr"]
    )
    for item in to_update + to_create:
        item.account_id = account_id
    safe_bulk_create(Campaign, to_create)
    update_fields = [val for val in CAMPAIGN_FIELDS_MAPPING.keys() if val not in {"id"}]
    Campaign.objects.bulk_update(to_update, fields=update_fields)


def get_campaign_report(client, predicates: dict = None, addl_fields: list = None) -> list:
    """
    Retrieve Adwords Campaign Performance report
    :param client: get_client function client result
    :param predicates: dict -> Adwords report selector predicates
    :param addl_fields: Additional report fields to retrieve
    :return: list -> namedtuples
    """
    fields = list(CAMPAIGN_FIELDS_MAPPING.values()) + list(addl_fields or [])
    predicates = predicates or [{"field": "ServingStatus", "operator": "EQUALS", "values": ["SERVING"]}]
    selector = {"fields": fields, "predicates": predicates}
    report = _get_report(client, "CAMPAIGN_PERFORMANCE_REPORT", selector,
                         use_raw_enum_values=True, skip_column_header=True)
    rows = _output_to_rows(report, fields)
    return rows
