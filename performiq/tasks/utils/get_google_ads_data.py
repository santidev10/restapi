from aw_reporting.adwords_reports import MAIN_STATISTICS_FILEDS, COMPLETED_FIELDS
from aw_reporting.adwords_reports import placement_performance_report
from es_components.models import Channel
from es_components.managers import ChannelManager
from es_components.constants import SortDirections
from es_components.constants import SUBSCRIBERS_FIELD
from performiq.tasks import google_ads_update_scheduler
from performiq.analyzers.base_analyzer import GoogleAdsAnalyzer
from performiq.tasks.google_ads_scheduler import google_ads_update_scheduler
from performiq.tasks import update_campaigns_task
from performiq.utils.adwords_report import get_campaign_report
from performiq.models import OAuthAccount
from performiq.models import Campaign
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
    ]
    fields = ("AdGroupId", "Date", "Device", "Criteria", "DisplayName", "Ctr", "AverageCpm", "AverageCpv",
              "ActiveViewViewability") + MAIN_STATISTICS_FILEDS + COMPLETED_FIELDS
    report = placement_performance_report(client, predicates=predicates, fields=fields)
    # mapped_data = {}
    mapped_data = []
    for row in report:
        if "channel" not in row.DisplayName:
            continue
        data = {
            mapped_key: getattr(row, report_field, None) for report_field, mapped_key in data_mapping.items()
        }
        # mapped_data[data[CampaignDataFields.CHANNEL_ID]] = data
        mapped_data.append(data)
    return mapped_data
