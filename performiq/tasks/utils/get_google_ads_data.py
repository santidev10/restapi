from aw_reporting.adwords_reports import MAIN_STATISTICS_FILEDS, COMPLETED_FIELDS
from aw_reporting.adwords_reports import placement_performance_report
from performiq.models import IQCampaign
from performiq.models import IQCampaignChannel
# from performiq.models.constants import CampaignData
from performiq.models.constants import CampaignDataFields
from performiq.oauth_utils import get_client
from segment.api.serializers import CTLParamsSerializer
from segment.utils.query_builder import SegmentQueryBuilder
from segment.utils.bulk_search import bulk_search
from utils.utils import chunks_generator
from performiq.models import Account
from performiq.models import Campaign
from performiq.models import IQCampaignChannel
from utils.db.functions import safe_bulk_create



data_mapping = {
    "Impressions": CampaignDataFields.IMPRESSIONS,
    "VideoViews": CampaignDataFields.VIDEO_VIEWS,
    "Criteria": CampaignDataFields.CHANNEL_ID,
    "Ctr": CampaignDataFields.CTR,
    "AverageCpm": CampaignDataFields.CPM,
    "AverageCpv": CampaignDataFields.CPV,
    "ActiveViewViewability": CampaignDataFields.ACTIVE_VIEW_VIEWABILITY,

}


def get_google_ads_data(iq_campaign: IQCampaign):
    """
    Retrieve Placement Report for Campaign from Adwords API
    Returns dictionary of mapped keys to raw API data values. This is done so that data from multiple
        sources e.g. Google Ads, DV360 can be accessed with unified keys as API field names may differ.
        Preserve raw API data as it may be needed in it's raw form during processing
    """
    campaign_id = iq_campaign.campaign_id
    campaign = Campaign.objects.get(id=campaign_id)
    account = campaign.account
    client = get_client(
        client_customer_id=account.id,
        refresh_token=account.oauth_account.refresh_token,
    )
    predicates = [
        {"field": "AdNetworkType1", "operator": "EQUALS", "values": ["YOUTUBE_WATCH"]},
        {"field": "CampaignId", "operator": "EQUALS", "values": [campaign.id]},
        {"field": "Impressions", "operator": "GREATER_THAN", "values": 100},
    ]
    fields = ("AdGroupId", "Date", "Device", "Criteria", "DisplayName", "Ctr", "AverageCpm", "AverageCpv",
              "ActiveViewViewability") + MAIN_STATISTICS_FILEDS + COMPLETED_FIELDS
    report = placement_performance_report(client, predicates=predicates, fields=fields)
    all_data = []
    for row in report:
        if "channel" not in row.DisplayName:
            continue
        # Create new dictionary of mapped keys to raw API data values
        data = {
            mapped_key: getattr(row, report_field, None) for report_field, mapped_key in data_mapping.items()
        }
        all_data.append(data)
    return all_data
