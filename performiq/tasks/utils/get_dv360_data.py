import csv

from googleapiclient import discovery
from performiq.analyzers.constants import COERCE_FIELD_FUNCS
from performiq.utils.dv360 import load_credentials
from performiq.utils.dv360 import get_discovery_resource
from performiq.models import OAuthAccount
from performiq.models import Campaign
from performiq.models.constants import OAuthType
from performiq.models.constants import CampaignDataFields
from utils.exception import backoff

"""
CANNOT GET:
METRIC_CLIENT_COST_VIEWABLE_ECPM_ADVERTISER_CURRENCY = CampaignDataFields.CPM
METRIC_CTR = CampaignDataFields.CTR
METRIC_ACTIVE_VIEW_PERCENT_VISIBLE_ON_COMPLETE = CampaignDataFields.VIDEO_QUARTILE_100_RATE,
METRIC_ACTIVE_VIEW_PCT_VIEWABLE_IMPRESSIONS = CampaignDataField.ACTIVE_VIEW_VIEWABILITY
"""

KEY_MAPPING = {
    "FILTER_ADVERTISER": CampaignDataFields.ADVERTISER_ID,
    "FILTER_ADVERTISER_CURRENCY": "advertiser_currency",
    "FILTER_PLACEMENT_ALL_YOUTUBE_CHANNELS": CampaignDataFields.CHANNEL_ID,
    "METRIC_TRUEVIEW_VIEW_RATE": CampaignDataFields.VIDEO_VIEW_RATE,
    "METRIC_CLIENT_COST_ECPM_ADVERTISER_CURRENCY": CampaignDataFields.CPM,
    "METRIC_TRUEVIEW_CPV_ADVERTISER": CampaignDataFields.CPV,
    "METRIC_CLIENT_COST_ADVERTISER_CURRENCY": CampaignDataFields.COST,
}


def get_dv360_data(iq_campaign, **kwargs):
    dv360_campaign = Campaign.objects.get(id=iq_campaign.campaign.id, oauth_type=OAuthType.DV360.value)
    account = OAuthAccount.objects.get(id=kwargs["oauth_account_id"])
    credentials = load_credentials(account)
    resource = get_discovery_resource(credentials)
    insertion_order_ids = get_insertion_orders(resource, dv360_campaign.id)[:1]

    # report_response = get_metrics(credentials, dv360_campaign.advertiser.id, insertion_order_ids)
    # report_url = report_response["metadata"]["googleCloudStoragePathForLatestReport"]
    # with open("output", "wb") as file,\
    #         closing(urlopen(report_url)) as url:
    #     file.write(url.read())
    report_response = {
        "params": {
            "groupBys": [
                "FILTER_ADVERTISER",
                "FILTER_ADVERTISER_CURRENCY",
                "FILTER_PLACEMENT_ALL_YOUTUBE_CHANNELS"
            ],
            "metrics": [
                "METRIC_TRUEVIEW_VIEW_RATE",
                "METRIC_CLIENT_COST_ECPM_ADVERTISER_CURRENCY",
                "METRIC_TRUEVIEW_CPV_ADVERTISER",
                "METRIC_CLIENT_COST_ADVERTISER_CURRENCY"
            ],
            "type": "TYPE_TRUEVIEW"
        },
    }
    mapped_data = process_csv(report_response)
    return mapped_data
    # process_csv(report_response["params"])
    #


def get_metrics(credentials, advertiser_id, insertion_order_ids):
    service = discovery.build("doubleclickbidmanager", "v1.1", credentials=credentials)
    insertion_order_filters = [{
        "type": "FILTER_INSERTION_ORDER",
        "value": _id
    } for _id in insertion_order_ids]
    report_request = {
        "params": {
            "type": "TYPE_TRUEVIEW",
            "metrics": [
                "METRIC_TRUEVIEW_VIEW_RATE",
                "METRIC_CLIENT_COST_ECPM_ADVERTISER_CURRENCY",
                "METRIC_TRUEVIEW_CPV_ADVERTISER",
                "METRIC_CLIENT_COST_ADVERTISER_CURRENCY",
            ],
            "groupBys": [
                "FILTER_ADVERTISER",
                "FILTER_ADVERTISER_CURRENCY",
                "FILTER_PLACEMENT_ALL_YOUTUBE_CHANNELS",
            ],
            "filters": [
                {
                    "type": "FILTER_ADVERTISER",
                    "value": advertiser_id,
                },
                *insertion_order_filters,
            ],
        },
        "metadata": {
            "title": "DV360 Automation API-generated report",
            "dataRange": "LAST_90_DAYS",
            "format": "csv"
        },
        "schedule": {
            "frequency": "ONE_TIME"
        }
    }
    operation = service.queries().createquery(body=report_request).execute()
    query_request = service.queries().getquery(queryId=operation["queryId"])
    response = get_dv360_query_completion(query_request)
    return response


@backoff(max_backoff=3600 * 5, exceptions=(ValueError,))
def get_dv360_query_completion(query_request):
    response = query_request.execute()
    if response["metadata"]["running"] is True and response["metadata"].get("googleCloudStoragePathForLatestReport") \
            is None:
        raise ValueError
    return response


def get_insertion_orders(resource, dv360_campaign_id):
    res = resource.advertisers().insertionOrders().list(advertiserId="1878225",
                                                        filter=f"campaignId={dv360_campaign_id}").execute()
    insertion_order_ids = [item["insertionOrderId"] for item in res.get("insertionOrders", [])]
    return insertion_order_ids


def process_csv(query):
    columns = query["params"]["groupBys"] + query["params"]["metrics"]
    fp = "output"
    header_skipped = False
    all_rows = []
    with open(fp, mode="r") as file:
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
                if mapped_data_key == CampaignDataFields.CHANNEL_ID:
                    # Youtube Placment URL does not need to be preserved
                    row[index] = row[index].split("/channel/")[-1]
                api_value = row[index]
                coercer = COERCE_FIELD_FUNCS.get(mapped_data_key)
                mapped_value = coercer(api_value) if coercer is not None else api_value
                formatted[mapped_data_key] = mapped_value
            all_rows.append(formatted)
    return all_rows
