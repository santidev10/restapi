from collections import namedtuple
from typing import Dict
from typing import List

from performiq.analyzers.constants import COERCE_FIELD_FUNCS
from aw_reporting.adwords_reports import _get_report
from aw_reporting.adwords_reports import _output_to_rows
from aw_reporting.adwords_reports import stream_iterator
from oauth.utils.client import load_client_settings
from oauth.utils.client import get_client
from oauth.utils.client import API_VERSION


def get_aw_customers(refresh_token):
    config = load_client_settings()
    aw_client = get_client(
        client_customer_id=None,
        refresh_token=refresh_token,
        **config
    )
    customer_service = aw_client.GetService(
        "CustomerService", version=API_VERSION
    )
    return customer_service.getCustomers()


def get_campaign_report(client, fields, predicates: list = None, date_range: dict = None) -> list:
    """
    Retrieve Adwords Campaign Performance report
    :param client: get_client function client result
    :param predicates: dict -> Adwords report selector predicates
    :param date_range: dict -> Date range for report
    :return: list -> namedtuples
    """
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


def get_accounts(refresh_token):
    gads_accounts = get_customers(refresh_token)
    mcc_accounts = []
    cid_accounts = []
    for account in gads_accounts:
        if account["canManageClients"] and not account["testAccount"]:
            container = mcc_accounts
        else:
            container = cid_accounts
        container.append(account)
    return mcc_accounts, cid_accounts


def get_customers(refresh_token):
    config = load_client_settings()
    aw_client = get_client(
        client_customer_id=None,
        refresh_token=refresh_token,
        **config
    )
    customer_service = aw_client.GetService(
        "CustomerService", version=API_VERSION
    )
    return customer_service.getCustomers()


def prepare_items(report: List[namedtuple], model, fields_mapping: Dict[str, str],
                  ouath_type: int, defaults: Dict[str, int] = None) -> tuple:
    """
    Prepare items to be updated or created for PerformIQ models. Metric numerical fields returned as string values
        from the API will be coerced using COERCE_FIELD_FUNCS

        fields_mapping dict should contain all Model fields to be updated from report
        ** fields_mapping must contain id key
        Each obj key will be set on the model obj using it's report value
        e.g. Updating Campaign impressions, video_views
            fields_mapping = {
                "impressions": "Impressions",
                "video_views": "VideoViews"
            }
    :param report: iter -> Iterable that contains report rows
    :param model: Model objects to instantiate from report values
    :param fields_mapping: dict -> Mapping of model field to report fields e.g. id: CampaignId
    :param ouath_type: OAuthType enum value
    :param defaults: dict -> Additional attributes to be set on all Model objects
    :return: tuple
    """
    defaults = defaults or {}
    exists_mapping = {
        item.id: item
        for item in model.objects.filter(
            oauth_type=0,
            id__in={*[getattr(item, fields_mapping["id"]) for item in report]}
        )
    }
    to_update = []
    to_create = []
    for row in report:
        try:
            obj = exists_mapping[int(getattr(row, fields_mapping["id"]))]
            container = to_update
        except KeyError:
            default = {"oauth_type": ouath_type}
            obj = model(**default)
            container = to_create
        # Set attributes that should be set defined in defaults
        [setattr(obj, model_field, value) for model_field, value in defaults.items()]
        for model_field, report_field in fields_mapping.items():
            coercer = COERCE_FIELD_FUNCS.get(model_field)
            report_value = getattr(row, report_field, None)
            mapped_value = coercer(report_value) if coercer else report_value
            setattr(obj, model_field, mapped_value)
        container.append(obj)
    return to_update, to_create
