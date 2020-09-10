from performiq.models import Campaign

from googleads import adwords
from performiq.utils.constants import CAMPAIGN_FIELDS_MAPPING
from performiq.utils.update import update


def update_campaigns(account_id, fields=None, report_query=None):
    """
    Update campaigns by retrieving report data and creating / updating items
    :param account_id: int -> Google Ads CID account id
    :param fields: list -> Adwords Campaign Performance Report fields
    :param report_query: str -> Adwords Query Language string
    :return:
    """
    report_query = report_query or _get_default_query()
    fields = fields or CAMPAIGN_FIELDS_MAPPING.keys()
    items = update(account_id, fields, CAMPAIGN_FIELDS_MAPPING, report_query, Campaign)
    return items


def _get_default_query():
    report_query = (
        adwords.ReportQueryBuilder()
            .From("CAMPAIGN_PERFORMANCE_REPORT")
            .Where("ServingStatus")
            .EqualTo("SERVING")
            .Select(*CAMPAIGN_FIELDS_MAPPING.values())
            .Build()
    )
    return report_query
