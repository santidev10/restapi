from aw_reporting.adwords_reports import _get_report
from aw_reporting.adwords_reports import _output_to_rows
from aw_reporting.adwords_reports import stream_iterator
from performiq.utils.constants import CAMPAIGN_FIELDS_MAPPING
from performiq.models import OAuthAccount

from aw_reporting.adwords_api import get_web_app_client


def get_campaign_report(client, predicates: dict = None, date_range: dict = None, addl_fields: list = None) -> list:
    """
    Retrieve Adwords Campaign Performance report
    :param client: get_client function client result
    :param predicates: dict -> Adwords report selector predicates
    :param date_range: dict -> Date range for report
    :param addl_fields: Additional report fields to retrieve
    :return: list -> namedtuples
    """
    fields = list(CAMPAIGN_FIELDS_MAPPING.values()) + list(addl_fields or [])
    predicates = predicates or [{"field": "ServingStatus", "operator": "EQUALS", "values": ["SERVING"]}]
    selector = {"fields": fields, "predicates": predicates}
    date_range_type = "ALL_TIME"
    if date_range:
        date_range_type = "CUSTOM_DATE"
        selector["dateRange"] = date_range
    report = _get_report(client, "CAMPAIGN_PERFORMANCE_REPORT", selector, date_range_type=date_range_type,
                         use_raw_enum_values=True, skip_column_header=True)
    rows = _output_to_rows(report, fields)
    return rows


def get_client(account_id, ouath_account_id=None):
    if ouath_account_id is None:
        oauth_id = account_id
    else:
        oauth_id = ouath_account_id
    oauth_account = OAuthAccount.objects.get(id=oauth_id)
    client = get_web_app_client(
        refresh_token=oauth_account.refresh_token,
        client_customer_id=account_id
    )
    return client


def get_report(client, report_query, fields, addl_fields=None):
    opts = dict(
        use_raw_enum_values=True,
        skip_report_header=True,
        skip_column_header=True,
        skip_report_summary=True,
    )
    fields = list(fields) + (addl_fields or [])
    report_downloader = client.GetReportDownloader(version='v201809')
    report = report_downloader.DownloadReportAsStreamWithAwql(report_query, 'CSV', **opts)
    result = _output_to_rows(stream_iterator(report), fields)
    return result
