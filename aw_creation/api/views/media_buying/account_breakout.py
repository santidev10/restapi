from django.db.models import F
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response

from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_creation.models import AccountCreation
from ads_analyzer.reports.account_targeting_report.constants import ReportType
from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_creation.api.views.media_buying.constants import TARGETING_MAPPING
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import validate_targeting
from aw_creation.api.serializers.media_buying.campaign_setting_serialzier import CampaignSettingSerializer
from aw_reporting.models import Campaign
from aw_reporting.models import AdGroup
from utils.views import validate_fields


class AccountCampaignBreakoutAPIView(APIView):
    """
    GET: Retrieve campaign breakout details
    POST: Create breakout campaigns
    """
    REQUIRED_AD_FIELDS = ("video_url", "display_url", "final_url", "tracking_template", "companion_banner", "video_ad_format")

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        params = request.query_params
        ad_group_ids = params["ad_group_ids"]
        if isinstance(ad_group_ids, str):
            ad_group_ids = [ad_group_ids]
        ad_groups = AdGroup.objects\
            .filter(id__in=ad_group_ids)\
            .values("id", "campaign_id", "cpv_bid", "cpm_bid", "cpc_bid")
        campaigns = Campaign.objects.filter(id__in=[item["campaign_id"] for item in ad_groups])\
            .annotate(salesforce_goal_id=F("salesforce_placement__goal_type_id"))
        # Mapping of campaign_ids and ad_group bidding values
        campaign_ad_group_bid_mapping = {
            ad_group["campaign_id"]: ad_group
            for ad_group in ad_groups
        }
        serializer = CampaignSettingSerializer(campaigns, many=True, context={"bid_mapping": campaign_ad_group_bid_mapping})
        data = serializer.data
        return Response(data=data)

    def _get_filters(self, campaign_id):
        filters = {
            "ad_group__campaign_id": campaign_id
        }
        return filters

    def _get_account_creation(self, request, pk):
        user = request.user
        try:
            return AccountCreation.objects.user_related(user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404

    def post(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        body = request.body
        account_creation = self._get_account_creation(request, pk)
        account = account_creation.id
        settings = body["settings"]
        campaign_ids = body["campaign_ids"]
        should_pause = body["pause_campaigns"]
        updated_budget_value = body["updated_budget"]

        # If should pause, then pause the campaign_ids

        # If updated budget value, update campaigns budgets not in campaign_ids

        # Create campaign under the current cid
        campaign_creation = CampaignCreation

        # create ad group

        # create ad

    def _pause_campaigns(self, campaign_ids):
        pass

    def _update_budgets(self, campaign_ids):
        pass