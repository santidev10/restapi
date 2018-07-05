import csv
import logging
from collections import namedtuple
from time import sleep

from googleads.errors import AdWordsReportBadRequestError

from aw_reporting.adwords_api import API_VERSION

logger = logging.getLogger(__name__)

MAIN_STATISTICS_FILEDS = (
    "VideoViews", "Cost", "Clicks", "Impressions",
    "Conversions", "AllConversions", "ViewThroughConversions",
)

statistics_fields = ("VideoViewRate", "Ctr",
                     "AverageCpv", "AverageCpm") + MAIN_STATISTICS_FILEDS

COMPLETED_FIELDS = ("VideoQuartile25Rate", "VideoQuartile50Rate",
                    "VideoQuartile75Rate", "VideoQuartile100Rate")

CAMPAIGN_PERFORMANCE_REPORT_FIELDS = (
                                         "CampaignId", "CampaignName",
                                         "ServingStatus", "CampaignStatus",
                                         "StartDate", "EndDate", "Amount",
                                         "AdvertisingChannelType",
                                     ) + COMPLETED_FIELDS \
                                     + MAIN_STATISTICS_FILEDS
AD_GROUP_PERFORMANCE_REPORT_FIELDS = (
                                         "CampaignId",
                                         "AdGroupId", "AdGroupName",
                                         "AdGroupStatus", "AdGroupType",
                                         "Date", "Device", "AdNetworkType1",
                                         "AveragePosition",
                                         "ActiveViewImpressions", "Engagements"
                                     ) + MAIN_STATISTICS_FILEDS \
                                     + COMPLETED_FIELDS
GEO_LOCATION_REPORT_FIELDS = ("Id", "CampaignId", "CampaignName",
                              "IsNegative") + MAIN_STATISTICS_FILEDS

DAILY_STATISTIC_PERFORMANCE_REPORT_FIELDS = ("Criteria", "AdGroupId", "Date") \
                                            + MAIN_STATISTICS_FILEDS \
                                            + COMPLETED_FIELDS
AD_PERFORMANCE_REPORT_FIELDS = ("AdGroupId", "Headline", "Id",
                                "ImageCreativeName", "DisplayUrl", "Status",
                                "Date", "AveragePosition",
                                "CombinedApprovalStatus") \
                               + COMPLETED_FIELDS + MAIN_STATISTICS_FILEDS

AD_PERFORMANCE_REPORT_FIELDS = ("AdGroupId", "Headline", "Id",
                                "ImageCreativeName", "DisplayUrl", "Status",
                                "Date", "AveragePosition",
                                "CombinedApprovalStatus") \
                               + COMPLETED_FIELDS + MAIN_STATISTICS_FILEDS

EMPTY = " --"
MAX_ACCESS_AD_WORDS_TRIES = 5


def stream_iterator(stream):
    while True:
        line = stream.readline()
        if not line:
            break
        yield line.decode()


def _get_report(client, name, selector, date_range_type=None,
                include_zero_impressions=False, skip_column_header=True):
    report_downloader = client.GetReportDownloader(version=API_VERSION)

    report = {
        "reportName": name,
        "dateRangeType": date_range_type or "ALL_TIME",
        "reportType": name,
        "downloadFormat": "CSV",
        "selector": selector,
    }
    try_num = 0
    while True:
        try:
            try:
                result = report_downloader.DownloadReportAsStream(
                    report,
                    skip_report_header=True,
                    skip_column_header=skip_column_header,
                    skip_report_summary=True,
                    include_zero_impressions=include_zero_impressions
                )
            except AdWordsReportBadRequestError as e:
                logger.warning(client.client_customer_id)
                logger.warning(e)
                if e.type == "AuthorizationError.USER_PERMISSION_DENIED":
                    return
                elif e.type == "ReportDefinitionError.CUSTOMER_SERVING_TYPE_REPORT_MISMATCH":
                    return
                raise

        except Exception as e:
            error_str = str(e)
            if "RateExceededError.RATE_EXCEEDED" in error_str:
                raise

            logger.error("Error: %s" % error_str)
            if try_num < MAX_ACCESS_AD_WORDS_TRIES:
                try_num += 1
                seconds = try_num ** 4
                logger.info("Sleep for %d seconds" % seconds)
                sleep(seconds)
            else:
                return
        else:
            return stream_iterator(result)


def _get_csv_reader(output):
    if type(output) is str:
        output = output.strip(" \t\n\r").split("\n")
    return csv.reader(output, delimiter=",", dialect="excel")


def _output_to_rows(output, fields):
    if not output:
        return []
    reader = _get_csv_reader(output)
    row = namedtuple("Row", fields)
    rows = []
    for line in reader:
        rows.append(
            row(*line)
        )
    return rows


def account_performance_report(client):
    """
    includes all statistics aggregated by default at the account level
    we use it to check if an account needs to be updated
    (if stats have changed)
    :param client:
    :return:
    """
    fields = (
                 "AccountDescriptiveName", "AccountCurrencyCode",
                 "AccountTimeZone", "CanManageClients",
                 "CustomerDescriptiveName",
                 "ExternalCustomerId", "IsTestAccount",
             ) + MAIN_STATISTICS_FILEDS

    result = _get_report(
        client,
        "ACCOUNT_PERFORMANCE_REPORT",
        {
            "fields": fields,
            "predicates": [],
        },
        date_range_type="ALL_TIME"
    )

    return _output_to_rows(result, fields)


def placement_performance_report(client, dates=None):
    """
    Used for getting channels and managed videos
    :param client:
    :param dates:
    :return:
    """
    fields = ("AdGroupId", "Date", "Device", "Criteria", "DisplayName") + \
             MAIN_STATISTICS_FILEDS + COMPLETED_FIELDS

    predicates = [
        {
            "field": "AdNetworkType1",
            "operator": "EQUALS",
            "values": ["YOUTUBE_WATCH"]
        },
    ]
    selector = {
        "fields": fields,
        "predicates": predicates,
    }
    if dates:
        date_range_type = "CUSTOM_DATE"
        selector["dateRange"] = {
            "min": dates[0].strftime("%Y%m%d"),
            "max": dates[1].strftime("%Y%m%d"),
        }
    else:
        date_range_type = "ALL_TIME"

    result = _get_report(
        client,
        "PLACEMENT_PERFORMANCE_REPORT",
        selector,
        date_range_type=date_range_type
    )

    return _output_to_rows(result, fields)


def geo_performance_report(client, dates=None, additional_fields=None):
    fields = ("CityCriteriaId", "CountryCriteriaId", "CampaignId")

    if additional_fields:
        fields += additional_fields

    predicates = [
        {
            "field": "IsTargetingLocation",
            "operator": "IN",
            "values": [True, False],
        },
        {
            "field": "LocationType",
            "operator": "EQUALS",
            "values": "LOCATION_OF_PRESENCE",
        },
    ]

    selector = {
        "fields": fields,
        "predicates": predicates,
    }

    date_range = {}
    if dates:
        if dates[0]:
            date_range["min"] = dates[0].strftime("%Y%m%d")
        if dates[1]:
            date_range["max"] = dates[1].strftime("%Y%m%d")
    if date_range:
        selector["dateRange"] = date_range

    result = _get_report(
        client, "GEO_PERFORMANCE_REPORT", selector,
        date_range_type="CUSTOM_DATE" if dates else "ALL_TIME",
    )
    return _output_to_rows(result, fields)


def geo_location_report(client):
    fields = GEO_LOCATION_REPORT_FIELDS
    selector = {
        "fields": fields,
        "predicates": [],
    }
    result = _get_report(
        client, "CAMPAIGN_LOCATION_TARGET_REPORT", selector,
        date_range_type="ALL_TIME",
        include_zero_impressions=True,
    )
    return _output_to_rows(result, fields)


def _daily_statistic_performance_report(client, name, dates=None,
                                        additional_fields=None):
    fields = DAILY_STATISTIC_PERFORMANCE_REPORT_FIELDS

    if additional_fields:
        fields += additional_fields

    selector = {
        "fields": fields,
        "predicates": []
    }
    if dates:
        selector["dateRange"] = {
            "min": dates[0].strftime("%Y%m%d"),
            "max": dates[1].strftime("%Y%m%d"),
        }
    result = _get_report(
        client, name, selector,
        date_range_type="CUSTOM_DATE" if dates else "ALL_TIME"
    )
    return _output_to_rows(result, fields)


def gender_performance_report(client, dates):
    return _daily_statistic_performance_report(
        client, "GENDER_PERFORMANCE_REPORT", dates
    )


def parent_performance_report(client, dates):
    return _daily_statistic_performance_report(
        client, 'PARENTAL_STATUS_PERFORMANCE_REPORT', dates
    )


def age_range_performance_report(client, dates):
    return _daily_statistic_performance_report(
        client, "AGE_RANGE_PERFORMANCE_REPORT", dates
    )


def keywords_performance_report(client, dates):
    return _daily_statistic_performance_report(
        client, "DISPLAY_KEYWORD_PERFORMANCE_REPORT", dates
    )


def topics_performance_report(client, dates):
    return _daily_statistic_performance_report(
        client, "DISPLAY_TOPICS_PERFORMANCE_REPORT", dates,
    )


def audience_performance_report(client, dates):
    return _daily_statistic_performance_report(
        client, "AUDIENCE_PERFORMANCE_REPORT", dates,
        additional_fields=("UserListName",)
    )


def ad_performance_report(client, dates=None):
    fields = AD_PERFORMANCE_REPORT_FIELDS

    selector = {
        "fields": fields,
        "predicates": [],
    }
    if dates:
        selector["dateRange"] = {
            "min": dates[0].strftime("%Y%m%d"),
            "max": dates[1].strftime("%Y%m%d"),
        }

    result = _get_report(
        client, "AD_PERFORMANCE_REPORT",
        selector,
        date_range_type="CUSTOM_DATE" if dates else "ALL_TIME",
    )
    return _output_to_rows(result, fields)


def campaign_performance_report(client,
                                dates=None,
                                fields=None,
                                include_zero_impressions=True,
                                additional_fields=None):
    if fields is None:
        fields = CAMPAIGN_PERFORMANCE_REPORT_FIELDS
    fields = list(fields)
    if additional_fields:
        fields.extend(additional_fields)
    selector = {
        "fields": fields,
        "predicates": [],
    }
    if dates:
        selector["dateRange"] = {
            "min": dates[0].strftime("%Y%m%d"),
            "max": dates[1].strftime("%Y%m%d"),
        }
        date_range_type = "CUSTOM_DATE"
    else:
        date_range_type = "ALL_TIME"

    result = _get_report(
        client, "CAMPAIGN_PERFORMANCE_REPORT", selector,
        date_range_type=date_range_type,
        include_zero_impressions=include_zero_impressions,
    )
    return _output_to_rows(result, fields)


def ad_group_performance_report(client, dates=None):
    fields = AD_GROUP_PERFORMANCE_REPORT_FIELDS

    selector = {
        "fields": fields,
        "predicates": [],
    }
    if dates:
        selector["dateRange"] = {
            "min": dates[0].strftime("%Y%m%d"),
            "max": dates[1].strftime("%Y%m%d"),
        }

    result = _get_report(
        client, "ADGROUP_PERFORMANCE_REPORT", selector,
        date_range_type="CUSTOM_DATE" if dates else "ALL_TIME",
    )
    return _output_to_rows(result, fields)


def video_performance_report(client, dates=None):
    main_stats = tuple(set(MAIN_STATISTICS_FILEDS) - {"AllConversions"})
    fields = ("VideoChannelId", "VideoDuration", "VideoId", "AdGroupId",
              "Date") + main_stats + COMPLETED_FIELDS

    selector = {
        "fields": fields,
        "predicates": [],
    }
    if dates:
        selector["dateRange"] = {
            "min": dates[0].strftime("%Y%m%d"),
            "max": dates[1].strftime("%Y%m%d"),
        }

    result = _get_report(
        client, "VIDEO_PERFORMANCE_REPORT",
        selector,
        date_range_type="CUSTOM_DATE" if dates else "ALL_TIME",
        include_zero_impressions=False,
    )
    return _output_to_rows(result, fields)
