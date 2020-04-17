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
        Retrieve settings for Media Buying Campaign Breakout
        Settings are simplified compositions of Campaign, AdGroup,
        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        pk = kwargs["pk"]
        account_creation = get_account_creation(request.user, pk)
        account = account_creation.account
        params = request.query_params
        ad_group_ids = params["ad_group_ids"].split(",")
        # Get AdGroup settings
        ad_groups = AdGroup.objects\
            .filter(campaign__account=account, id__in=ad_group_ids)\
            .values("id", "campaign_id", "cpv_bid", "cpm_bid", "cpc_bid")
        # Get Campaign settings
        campaigns = Campaign.objects.filter(account=account, id__in=[item["campaign_id"] for item in ad_groups])\
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
        Create new campaign from existing targeting
        Request body should contain data used by CampaignBreakoutSerializer to create
            CampaignCreation, AdGroupCreation, and AdCreations to update on GoogleAds
        Updates Campaign budgets and pauses AdGroups if required
        :return:
        """
        pk = kwargs["pk"]
        data = request.data
        account_creation = get_account_creation(request.user, pk)
        settings = data["settings"]

        # Handles creation of all creation items
        serializer = CampaignBreakoutSerializer(data=settings, context={"account_creation": account_creation})
        serializer.is_valid(raise_exception=True)
        serializer.save()
        self._process(account_creation, data)
        return Response()

    def _process(self, account_creation, data):
        """
        Update Campaign budgets and AdGroup status
        :param account_creation: AccountCreation
        :param data: dict: Request body from post method
        :return:
        """
        should_pause_non_breakout_ad_groups = data.get("should_pause")
        breakout_ad_group_ids = data.get("breakout_ad_group_ids", [])
        updated_campaign_budget = data.get("updated_campaign_budget", None)

        if should_pause_non_breakout_ad_groups:
            # Pause all break out ad groups that are broken out into new campaign
            params = {}
            breakout_ad_groups = AdGroup.objects.filter(id__in=breakout_ad_group_ids).annotate(campaign_creation=F("campaign__campaign_creation"))
            breakout_campaigns = Campaign.objects.filter(id__in=breakout_ad_groups.values_list("campaign_id", flat=True).distinct())
            self._create_campaign_creations(breakout_campaigns.values(), account_creation)
            self._create_ad_group_creations(breakout_ad_groups.values(), **params)

        if updated_campaign_budget:
            # Get campaigns of non breakout ad groups and update their budgets
            params = {"budget": updated_campaign_budget}
            non_breakout_campaigns = Campaign.objects.filter(account=account_creation.account).exclude(ad_groups__id__in=breakout_ad_group_ids).distinct()
            self._create_campaign_creations(non_breakout_campaigns.values(), account_creation, **params)

    def _create_campaign_creations(self, campaigns, account_creation, **params):
        """
        Create related CampaignCreation for campaigns for Google Ads sync
        :param campaigns: list: [Campaign, ...]
        :param account_creation: AccountCreation
        :param params: dict: Optional create / update key, values
        :return:
        """
        campaign_creations = []
        for campaign in campaigns:
            defaults = {
                "account_creation": account_creation,
                "name": campaign["name"],
                "budget": campaign["budget"],
                "start": campaign["start_date"],
                "end": campaign["end_date"],
            }
            defaults.update(params or {})
            creation, _ = CampaignCreation.objects.update_or_create(campaign_id=campaign["id"], defaults=defaults)
            campaign_creations.append(creation)
        return campaign_creations

    def _create_ad_group_creations(self, ad_groups, params=None):
        """
        Create related AdGroupCreation for ad_groups for Google Ads sync
        :param ad_groups: list: [AdGroup, ...]
        :param params: dict: Optional create / update key, values
        :return:
        """
        creations = []
        for ag in ad_groups:
            defaults = {
                "campaign_creation_id": ag["campaign_creation"],
                "name": ag["name"],
            }
            defaults.update(params or {})
            creation, _ = AdGroupCreation.objects.update_or_create(ad_group_id=ag["id"], defaults=defaults)
            creations.append(creation)
        return creations
