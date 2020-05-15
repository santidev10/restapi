from django.db.models import F
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_creation.api.serializers.media_buying.campaign_setting_serializer import CampaignSettingSerializer
from aw_creation.api.serializers.media_buying.campaign_breakout_serializer import CampaignBreakoutSerializer
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import BID_STRATEGY_TYPE_MAPPING

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

        # Handles creation of new campaign and ad group creation breakouts
        serializer = CampaignBreakoutSerializer(data=data, context={"account_creation": account_creation})
        serializer.is_valid(raise_exception=True)
        breakout_campaign = serializer.save()
        self._process(account_creation, breakout_campaign, data)
        return Response()

    def _process(self, account_creation, breakout_campaign, data):
        """
        Update Campaign budgets and AdGroup status
        pause_old_ad_groups determines if all ad_groups that are not part of breakout should be paused
        :param account_creation: AccountCreation
        :param data: dict: Request body from post method
        :return:
        """
        should_pause_source_ad_groups = data.get("pause_source_ad_groups", False)
        breakout_ad_group_ids = data.get("ad_group_ids", [])
        updated_campaign_budget = data.get("updated_campaign_budget", None)

        # Pause ad groups that were used to create new breakout campaign
        if should_pause_source_ad_groups:
            params = {"status": 0}
            # Exclude the new breakout ad group that shares ad group idd
            source_ad_groups = AdGroup.objects \
                .filter(id__in=breakout_ad_group_ids) \
                .exclude(campaign__campaign_creation=breakout_campaign)
            all_campaign_ids = Campaign.objects \
                .filter(id__in=source_ad_groups.values_list("campaign_id", flat=True)) \
                .values_list("id", flat=True) \
                .distinct()
            existing_campaign_creations = CampaignCreation.objects.filter(campaign_id__in=all_campaign_ids)
            source_campaign_ids = set(all_campaign_ids) - set(cc.campaign_id for cc in existing_campaign_creations)
            source_campaigns = Campaign.objects.filter(id__in=source_campaign_ids)
            # campaign creations might already exist from different ad groups
            # Need to create CampaignCreation's first for AdGroupCreation relations
            campaign_creations = self._create_campaign_creations(source_campaigns, account_creation)
            all_campaign_creations = campaign_creations + list(existing_campaign_creations)
            self._create_ad_group_creations(all_campaign_creations, source_ad_groups, params)

        if updated_campaign_budget is not None:
            # Get campaigns of non breakout ad groups and update their budgets
            params = {"budget": updated_campaign_budget}
            non_breakout_campaigns = Campaign.objects \
                .filter(account=account_creation.account) \
                .exclude(ad_groups__id__in=breakout_ad_group_ids) \
                .distinct()
            existing_creations = CampaignCreation.objects \
                .filter(campaign__in=non_breakout_campaigns)
            existing_creations.update(**params)
            source_campaigns = non_breakout_campaigns \
                .exclude(id__in=existing_creations
                         .values_list("campaign_id", flat=True))
            self._create_campaign_creations(source_campaigns, account_creation, params)

    def _create_campaign_creations(self, campaigns, account_creation, params=None):
        """
        Create or update related CampaignCreation for campaigns for Google Ads sync
        :param campaigns: list: [Campaign, ...]
        :param account_creation: AccountCreation
        :param params: dict: Optional create / update key, values
        :return:
        """
        params = params or {}
        to_create = []
        for campaign in campaigns:
            base_params = dict(
                account_creation=account_creation,
                campaign=campaign,
                name=campaign.name,
                start=campaign.start,
                end=campaign.end,
                budget=campaign.budget,
                type=campaign.type.upper() if campaign.type in {"Video", "Display"} else CampaignCreation.VIDEO_TYPE,
                bid_strategy_type=BID_STRATEGY_TYPE_MAPPING.get(campaign.bidding_strategy_type,
                                                                CampaignCreation.MAX_CPV_STRATEGY),
            )
            base_params.update(params)
            creation = CampaignCreation(
                **base_params
            )
            to_create.append(creation)
        CampaignCreation.objects.bulk_create(to_create)
        return to_create

    def _create_ad_group_creations(self, campaign_creations, ad_groups, params=None):
        """
        Create or update related AdGroupCreation for ad_groups for Google Ads sync
        Each AdGroup in ad_groups is annotated with its Campaign's CampaignCreation id
        :param ad_groups: list: [AdGroup, ...]
        :param params: dict: Optional create / update key, values
        :return:
        """
        id_to_creation_mapping = {
            creation.campaign_id: creation
            for creation in campaign_creations
        }
        params = params or {}
        to_create = []
        for ad_group in ad_groups:
            base_params = dict(
                campaign_creation=id_to_creation_mapping[ad_group.campaign_id],
                ad_group=ad_group,
                name=ad_group.name,
            )
            base_params.update(params)
            creation = AdGroupCreation(
                **base_params
            )
            to_create.append(creation)
        AdGroupCreation.objects.bulk_create(to_create)
        return to_create
