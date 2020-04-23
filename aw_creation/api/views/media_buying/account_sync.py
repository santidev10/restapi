from django.db.models import F
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.models import Account


class AccountSyncAPIView(APIView):
    def get(self, request, *args, **kwargs):
        """ Get data to sync on Google Ads """
        # account_creation_id = kwargs["pk"]
        # campaigns_to_sync = CampaignCreation.objects.filter(
        #     account_creation_id=account_creation_id,
        #     id=4976,
        #     # creation_type=CampaignCreation.OPTIMIZATION,
        #     sync_at=None
        # )
        # ad_groups_to_sync = AdGroupCreation.objects\
        #     .filter(campaign_creation_id__in=campaigns_to_sync.values_list("id", flat=True))
        # ads_to_sync = AdCreation.objects.filter(ad_group_creation_id__in=ad_groups_to_sync.values_list("id", flat=True))
        #
        # campaign_data = [campaign.get_sync_data() for campaign in campaigns_to_sync]
        # ad_group_data = [ad_group.get_sync_data() for ad_group in ad_groups_to_sync]
        # ads_data = [ad.get_sync_data(request) for ad in ads_to_sync]
        # data = {
        #     "campaigns": campaign_data,
        #     "ad_groups": ad_group_data,
        #     "ads": ads_data,
        # }
        # return Response(data)
        from django.db.models import Subquery
        from django.db.models import OuterRef
        account_id = kwargs["account_id"]

        campaign_creations = Account.objects.get(id=account_id).account_creation.campaign_creations.all()
        sync_data = []
        for creation in campaign_creations:
            data = {
                **creation.get_sync_data(),
                "ad_group_ids": AdGroupCreation.objects
                    .filter(campaign_creation_id=creation.id)
                    .values_list("ad_group_id", flat=True)
            }
            sync_data.append(data)
        return Response(sync_data)
        # Get camapigns creations of type 1 under accounts
        # for each of those campaigns, get their ad groups and ads

    def post(self, request, *args, **kwargs):
        """ Set sync times for creation items """
        now = timezone.now()
        data = request.data
        account_id = kwargs["account_id"]
        account_creation = AccountCreation.objects.get(account_id=account_id)
        campaign_ids = data["campaign_ids"]
        ad_group_ids = data["ad_group_ids"]
        ad_ids = data["ad_ids"]
        CampaignCreation.objects.filter(account_creation=account_creation, campaign_id__in=campaign_ids).update(sync_at=now)
        AdGroupCreation.objects.filter(ad_group_id__in=ad_group_ids).update(sync_at=now)
        AdCreation.objects.filter(ad_id__in=ad_ids).update(sync_at=now)
        return Response()
