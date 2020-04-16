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
from aw_creation.api.serializers.media_buying.campaign_setting_serializer import CampaignSettingSerializer
from aw_creation.api.serializers.media_buying.campaign_breakout_serializer import CampaignBreakoutSerializer
from aw_creation.models import CampaignCreation
from aw_creation.models import AdGroupCreation
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
        """
        Retrieve Campaign settings
        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        pk = kwargs["pk"]
        params = request.query_params
        ad_group_ids = params["ad_group_ids"].split(",")
        # Get AdGroup settings
        ad_groups = AdGroup.objects\
            .filter(id__in=ad_group_ids)\
            .values("id", "campaign_id", "cpv_bid", "cpm_bid", "cpc_bid")
        # Get Campaign settings
        campaigns = Campaign.objects.filter(id__in=[item["campaign_id"] for item in ad_groups])\
            .annotate(salesforce_goal_id=F("salesforce_placement__goal_type_id"))
        # Mapping of Campaign to AdGroup bidding values
        campaign_ad_group_bid_mapping = {
            ad_group["campaign_id"]: ad_group
            for ad_group in ad_groups
        }
        serializer = CampaignSettingSerializer(campaigns,
                                               many=True, context={"bid_mapping": campaign_ad_group_bid_mapping}
                                               )
        data = serializer.data
        return Response(data=data)

    def post(self, request, *args, **kwargs):
        """
        Create new breakout campaigns
        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        pk = kwargs["pk"]
        data = request.data
        account_creation = get_account_creation(request.user, pk)
        settings = data["settings"]

        # Handles creation of all creation items
        serializer = CampaignBreakoutSerializer(data=settings)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        # account = account_creation.id
        self._process_campaigns(account_creation, data)
        self._process_ad_groups(account_creation, data)



    def _process_campaigns(self, account_creation, data):
        """
        Update campaigns of breakout items
        Creates CampaignCreation objects if needed
        If updated_campaign_budget is provided, will update campaign budgets of breakout items
        :param account:
        :param data:
        :return:
        """
        ad_group_ids = data.get("ad_group_ids", [])
        updated_campaign_budget = data.get("updated_campaign_budget", None)
        excluded_campaign_ids = Campaign.objects \
            .filter(account=account_creation.account) \
            .exclude(ad_groups__id__in=ad_group_ids) \
            .distinct()
        # set daily budgets of campaigns
        campaign_creations = []
        for campaign in excluded_campaign_ids:
            defaults = {
                "account_creation": account_creation,
                "name": campaign.name,
                "budget": updated_campaign_budget if updated_campaign_budget is not None else campaign.budget,
                "start": campaign.start_date,
                "end": campaign.end_date,
            }
            creation, _ = CampaignCreation.objects.update_or_create(campaign=campaign, defaults=defaults)
            campaign_creations.append(creation)
        return campaign_creations

    def _process_ad_groups(self, account, data):
        """
        Update AdGroups of breakout items
        Creates AdGroupCreation items if needed
        If , will set AdGroups to pause during next sync with Google Ads
        :param account:
        :param data:
        :return:
        """
        # pause ad groups in ad_group_ids
        ad_group_ids = data.get("ad_group_ids", [])
        ad_groups = AdGroup.objects.filter(id__in=ad_group_ids).select_related("campaign")
        creations = []
        for ad_group in ad_groups:
            defaults = {
                "campaign_creation": ad_group.campaign.campaign_creation,
                "name": ad_group.name,
                # "status": "paused",
            }
            creation, _ = AdGroupCreation.objects.update_or_create(ad_group=ad_group, defaults=defaults)
            creations.append(creation)
        return creations
