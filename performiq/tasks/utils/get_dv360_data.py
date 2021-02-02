import csv
import logging
import os
import tempfile

from django.conf import settings

from performiq.analyzers.base_analyzer import PerformIQDataFetchError
from performiq.analyzers.constants import COERCE_FIELD_FUNCS
from performiq.models import OAuthAccount
from performiq.models import IQCampaign
from performiq.models.constants import AnalysisFields
from utils.dv360_api import DV360Connector


KEY_MAPPING = {
    # Filters
    "FILTER_ADVERTISER": AnalysisFields.ADVERTISER_ID,
    "FILTER_ADVERTISER_CURRENCY": "advertiser_currency",
    "FILTER_PLACEMENT_ALL_YOUTUBE_CHANNELS": AnalysisFields.CHANNEL_ID,

    # Metrics
    "METRIC_CLIENT_COST_ADVERTISER_CURRENCY": AnalysisFields.COST,
    "METRIC_CLIENT_COST_ECPM_ADVERTISER_CURRENCY": AnalysisFields.CPM,
    "METRIC_IMPRESSIONS": AnalysisFields.IMPRESSIONS,
    "METRIC_TRUEVIEW_CPV_ADVERTISER": AnalysisFields.CPV,
    "METRIC_TRUEVIEW_VIEW_RATE": AnalysisFields.VIDEO_VIEW_RATE,
    "METRIC_TRUEVIEW_VIEWS": AnalysisFields.VIDEO_VIEWS,
}


logger = logging.getLogger(__name__)


def get_dv360_data(iq_campaign: IQCampaign, **kwargs):
    report_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        dv360_campaign = iq_campaign.campaign
        oauth_account = OAuthAccount.objects.get(id=kwargs["oauth_account_id"])
        connector = DV360Connector(access_token=oauth_account.token, refresh_token=oauth_account.refresh_token)
        insertion_orders = connector.get_insertion_orders(advertiserId="1878225",
                                                          filter=f"campaignId={dv360_campaign.id}")
        insertion_order_filters = [{
            "type": "FILTER_INSERTION_ORDER",
            "value": item["insertionOrderId"]
        } for item in insertion_orders]
        report_query = dict(
            report_fp=report_fp,
            report_type="TYPE_TRUEVIEW",
            filters=[
                {
                    "type": "FILTER_ADVERTISER",
                    "value": dv360_campaign.advertiser.id,
                },
                *insertion_order_filters,
            ],
            metrics=[
                metric for metric in KEY_MAPPING.keys() if metric.startswith("METRIC")
            ],
            group_by=[
                "FILTER_ADVERTISER",
                "FILTER_ADVERTISER_CURRENCY",
                "FILTER_PLACEMENT_ALL_YOUTUBE_CHANNELS",
            ],
            date_range="ALL_TIME",
        )
        result = connector.download_metrics_report(**report_query)
        csv_generator = report_csv_generator(report_fp, result)
        return csv_generator
    except Exception:
        logger.exception(f"Error retrieving DV360 Metrics report for IQCampaign id: {iq_campaign.id}")
        raise PerformIQDataFetchError


def report_csv_generator(report_fp, report_result) -> iter:
    """
    Maps DV360 metrics report data to formatted dict key values using KEY_MAPPING
    DV360 metrics report structure is split into two parts by a new line, actual data and report metadata
    This function will only yield metric data until the newline is encountered
    Data that is yielded will be coerced into primitives using COERCE_FIELD_FUNCS
        depending on the report field (e.g. cpm -> float)
    :param report_fp: Downloaded report filepath
    :param report_result: API Response for report request
    :return: Generator for mapped data
    """
    columns = report_result["params"]["groupBys"] + report_result["params"]["metrics"]
    header_skipped = False
    with open(report_fp, mode="r") as file:
        reader = csv.reader(file)
        for row in reader:
            if header_skipped is False:
                row = next(reader)
                header_skipped = True
            if len(row) <= 0 or not row[0]:
                break
            formatted = {}
            # Construct dict for each row as entire csv is not formatted with columns
            for index, column_name in enumerate(columns):
                mapped_data_key = KEY_MAPPING[column_name]
                if mapped_data_key == AnalysisFields.CHANNEL_ID:
                    row[index] = row[index].split("/channel/")[-1]
                api_value = row[index]
                coercer = COERCE_FIELD_FUNCS.get(mapped_data_key)
                mapped_value = coercer(api_value) if coercer is not None else api_value
                formatted[mapped_data_key] = mapped_value
            yield formatted
    # Clean up mkstemp file after generator is exhausted
    try:
        os.remove(report_fp)
    except OSError:
        pass
