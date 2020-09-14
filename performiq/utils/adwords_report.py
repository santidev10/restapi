from aw_reporting.adwords_reports import _output_to_rows
from aw_reporting.adwords_reports import stream_iterator
from googleads import adwords
from performiq.models import OAuthAccount

from aw_reporting.adwords_api import get_web_app_client


def get_campaigns(client, fields=None):
    fields = fields or ("CampaignId", "ServingStatus")
    report_query = (
        adwords.ReportQueryBuilder()
            .Select(*fields)
            .From("CAMPAIGN_PERFORMANCE_REPORT")
            .Where("ServingStatus").EqualTo("SERVING")
            .Build()
    )
    report = get_report(client, report_query, fields)
    return report


def get_client(account_id):
    # account = OAuthAccount.objects.get(id=account_id)
    # client = get_web_app_client(
    #     refresh_token=account.refresh_token,
    #     client_customer_id=account_id
    # )
    client = get_web_app_client(
        refresh_token='1/dFSYu09IZl43oA8pPOLE_NbkSDgO-Wm5LwA_dlkQoWsNoYWpKb856YvPe91IqL9t',
        client_customer_id=5453761695
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
