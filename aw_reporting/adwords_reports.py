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
                                         "StartDate", "EndDate", "Amount", "TotalAmount",
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


class AWErrorType:
    NOT_ACTIVE = "AuthorizationError.CUSTOMER_NOT_ACTIVE"
    PERMISSIONS_DENIED = "AuthorizationError.USER_PERMISSION_DENIED"
    REPORT_TYPE_MISMATCH = "ReportDefinitionError.CUSTOMER_SERVING_TYPE_REPORT_MISMATCH"


FATAL_AW_ERRORS = (
    AWErrorType.PERMISSIONS_DENIED,
    AWErrorType.REPORT_TYPE_MISMATCH,
)
EMPTY = " --"
MAX_ACCESS_AD_WORDS_TRIES = 5


class AccountInactiveError(Exception):
    pass


class AWReport:
    AD_GROUP_PERFORMANCE_REPORT = "ADGROUP_PERFORMANCE_REPORT"


class DateRangeType:
    CUSTOM_DATE = "CUSTOM_DATE"
    ALL_TIME = "ALL_TIME"


def stream_iterator(stream):
    while True:
        line = stream.readline()
        if not line:
            break
        yield line.decode()


def date_formatted(dt):
    return dt.strftime("%Y%m%d")


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
    report["selector"]["fields"] = list(report["selector"]["fields"])
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
                if e.type == AWErrorType.NOT_ACTIVE:
                    raise AccountInactiveError()
                if e.type in FATAL_AW_ERRORS:
                    return
                raise
        except AccountInactiveError as ex:
            raise ex

        except Exception as e:
            error_str = str(e)
            if "RateExceededError.RATE_EXCEEDED" in error_str:
                raise
            if "invalid_grant" in error_str:
                logger.debug("(Error) Invalid grant faced. Skipping. Msg: {}".format(error_str))
                return
            logger.debug("Error: %s" % error_str)
            if try_num < MAX_ACCESS_AD_WORDS_TRIES:
                try_num += 1
                seconds = try_num ** 3
                logger.info("Sleep for %d seconds" % seconds)
                sleep(seconds)
            else:
                raise e
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


def _daily_statistic_performance_report(
        client, name, dates=None, additional_fields=None, fields=None):
    if fields is None:
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


def gender_performance_report(client, dates, fields=None):
    return _daily_statistic_performance_report(
        client, "GENDER_PERFORMANCE_REPORT", dates, fields=fields
    )


def parent_performance_report(client, dates):
    return _daily_statistic_performance_report(
        client, 'PARENTAL_STATUS_PERFORMANCE_REPORT', dates
    )


def age_range_performance_report(client, dates, fields=None):
    return _daily_statistic_performance_report(
        client, "AGE_RANGE_PERFORMANCE_REPORT", dates, fields=fields
    )


def keywords_performance_report(client, dates, fields=None):
    return _daily_statistic_performance_report(
        client, "DISPLAY_KEYWORD_PERFORMANCE_REPORT", dates, fields=fields
    )


def topics_performance_report(client, dates, fields=None):
    return _daily_statistic_performance_report(
        client, "DISPLAY_TOPICS_PERFORMANCE_REPORT", dates, fields=fields
    )


def audience_performance_report(client, dates, fields=None):
    return _daily_statistic_performance_report(
        client, "AUDIENCE_PERFORMANCE_REPORT", dates, fields=fields,
        additional_fields=("UserListName",)
    )


def ad_performance_report(client, dates=None, fields=None):
    if fields is None:
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


def ad_group_performance_report(client, dates=None, fields=None):
    if fields is None:
        fields = AD_GROUP_PERFORMANCE_REPORT_FIELDS

    selector = {
        "fields": fields,
        "predicates": [],
    }
    if dates:
        start, end = dates
        selector["dateRange"] = {
            "min": date_formatted(start),
            "max": date_formatted(end),
        }

    result = _get_report(
        client, AWReport.AD_GROUP_PERFORMANCE_REPORT, selector,
        date_range_type=DateRangeType.CUSTOM_DATE if dates else DateRangeType.ALL_TIME,
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
