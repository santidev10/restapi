from django.db.models import F
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_creation.api.serializers.media_buying.campaign_setting_serializer import CampaignSettingSerializer
from aw_creation.api.serializers.media_buying.campaign_breakout_serializer import CampaignBreakoutSerializer
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import update_or_create_campaign_creation
from aw_creation.api.views.media_buying.utils import update_or_create_ad_group_creation
from aw_creation.models import CampaignCreation
from aw_creation.models import AdGroupCreation
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign


class AccountBreakoutAPIView(APIView):
    """
    GET: Retrieve campaign breakout details
    POST: Create breakout campaigns
    """

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
        serializer = CampaignSettingSerializer(campaigns, many=True, context={"bid_mapping": campaign_ad_group_bid_mapping})
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

        # Handles creation of all creation items
        serializer = CampaignBreakoutSerializer(data=data, context={"account_creation": account_creation})
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return
        self._process(account_creation, data)
        return Response()

    def _process(self, account_creation, data):
        """
        Update Campaign budgets and AdGroup status
        :param account_creation: AccountCreation
        :param data: dict: Request body from post method
        :return:
        """
        should_pause_non_breakout_ad_groups = data.get("pause_old_ad_groups", False)
        breakout_ad_group_ids = data.get("ad_group_ids", [])
        updated_campaign_budget = data.get("updated_campaign_budget", None)

        if should_pause_non_breakout_ad_groups:
            # Pause all break out ad groups that are broken out into new campaign
            params = {}
            breakout_ad_groups = AdGroup.objects\
                .filter(id__in=breakout_ad_group_ids)\
                .annotate(campaign_creation=F("campaign__campaign_creation"))
            breakout_campaigns = Campaign.objects\
                .filter(id__in=breakout_ad_groups.values_list("campaign_id", flat=True).distinct())
            self._set_campaign_creations(breakout_campaigns.values(), account_creation)
            self._set_ad_group_creations(breakout_ad_groups.values(), params)

        if updated_campaign_budget is not None:
            # Get campaigns of non breakout ad groups and update their budgets
            params = {"budget": updated_campaign_budget}
            non_breakout_campaigns = Campaign.objects\
                .filter(account=account_creation.account)\
                .exclude(ad_groups__id__in=breakout_ad_group_ids).distinct()
            self._set_campaign_creations(non_breakout_campaigns, account_creation, params)

    def _set_campaign_creations(self, campaigns, account_creation, params=None):
        """
        Create related CampaignCreation for campaigns for Google Ads sync
        :param campaigns: list: [Campaign, ...]
        :param account_creation: AccountCreation
        :param params: dict: Optional create / update key, values
        :return:
        """
        params = params or {}
        creations = []
        for campaign in campaigns:
            campaign_creation = update_or_create_campaign_creation(account_creation, campaign, params=params)
            creations.append(campaign_creation)
        return creations

    def _set_ad_group_creations(self, ad_groups, params=None):
        """
        Create related AdGroupCreation for ad_groups for Google Ads sync
        Each AdGroup in ad_groups is annotated with its Campaign's CampaignCreation
        :param ad_groups: list: [AdGroup, ...]
        :param params: dict: Optional create / update key, values
        :return:
        """
        params = params or {}
        creations = []
        for ag in ad_groups:
            ad_group_creation = update_or_create_ad_group_creation(ag.campaign_creation, ag, params)
            creations.append(ad_group_creation)
        return creations
