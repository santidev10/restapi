from collections import defaultdict

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.utils import format_query

__all__ = [
    "DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS",
    "DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME",
    "format_click_types_report",
    "get_stats_with_click_type_data",
    "get_clicks_report"
]


def get_clicks_report(client, ga_service, account, min_date, max_date, resource_name="campaign"):
    """
    Retrieves click data for given resource
    :param client: Google Ads client
    :param ga_service: GoogleAdsService
    :param account: CID Account
    :param resource_name: Google Ads resource to query
    :return: Google ads API search response
    """
    click_type_enum = client.get_type("ClickTypeEnum", version="v2").ClickType
    # Copy performance fields and use parameters to get resource id
    query_fields = {
        resource_name: ("resource_name",),
        **constants.CLICKS_PERFORMANCE_FIELDS,
    }
    clicks_query_fields = format_query(query_fields)
    clicks_query = f"SELECT {clicks_query_fields} FROM {resource_name} WHERE segments.date BETWEEN '{min_date}' AND " \
                   f"'{max_date}'"
    clicks_performance = ga_service.search(account.id, query=clicks_query)
    click_type_data = format_click_types_report(clicks_performance, click_type_enum, resource_name)
    return click_type_data


def format_click_types_report(report, click_type_enum, resource_name):
    """
    Formats Google ads API response from iterable to dictionary with resource values as keys and click data as values
    :param report: click types report
    :param click_type_enum: Google ads enum to map enum id to string
    :param resource_name: Google ads resources (Campaign, AdGroup, Ad, ...)
    :return {"ad_group_id+unique_field+date": [Row(), Row() ...], ... }
    """
    if not report:
        return {}
    tracking_click_types = dict(TRACKING_CLICK_TYPES)
    tracking_click_types_keys = set(tracking_click_types.keys())
    result = defaultdict(dict)
    for row in report:
        click_type = click_type_enum.Name(row.segments.click_type)
        if click_type not in tracking_click_types_keys:
            continue
        # Aggregate counts for all click types
        key = prepare_click_type_report_key(row, resource_name)
        click_type_name = tracking_click_types.get(click_type)
        result[key][click_type_name] = result[key].get(click_type, 0) + int(row.metrics.clicks.value)
    return result


def get_stats_with_click_type_data(stats: dict, click_type_data: dict, row_obj, resource_name="campaign",
                                   ignore_a_few_records=False):
    """
    Merges resource statistics data with click report data
    :param stats: Row statistics dictionary derived from Google Ads API search result
    :param click_type_data: Click report data derived from Google Ads API search result mapped from list to dictionary
    :param row_obj: Row object from service query
    :param resource_name: Main field to retrieve id from
    :param ignore_a_few_records:
    :return:
    """
    if click_type_data:
        key = prepare_click_type_report_key(row_obj, resource_name)
        if ignore_a_few_records:
            try:
                key_data = click_type_data.pop(key)
            except KeyError:
                return stats
        else:
            key_data = click_type_data.get(key)
        if key_data:
            stats.update(key_data)
    return stats


def prepare_click_type_report_key(row, resource_name):
    """
    Create unique id with object id and date
    :param row:
    :param resource_name:
    :return:
    """
    return "{}/{}".format(getattr(row, resource_name).resource_name, row.segments.date.value)


TRACKING_CLICK_TYPES = (
    ("VIDEO_WEBSITE_CLICKS", "clicks_website"),
    ("VIDEO_CALL_TO_ACTION_CLICKS", "clicks_call_to_action_overlay"),
    ("VIDEO_APP_STORE_CLICKS", "clicks_app_store"),
    ("VIDEO_CARD_ACTION_HEADLINE_CLICKS", "clicks_cards"),
    ("VIDEO_END_CAP_CLICKS", "clicks_end_cap")
)
DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME = "Criteria"
DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS = (
    "AdGroupId",
    "Date",
    "Criteria",
    "Clicks",
    "ClickType",
)
