from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation


class AccountSyncAPIView(APIView):
    def get(self, request, *args, **kwargs):
        account_id = kwargs["account_id"]
        campaigns_to_sync = CampaignCreation.objects.filter(
            account_creation__account_id=account_id,
            sync_at=None
        )
        ad_groups_to_sync = AdGroupCreation.objects\
            .filter(campaign_creation_id__in=campaigns_to_sync.values_list("id", flat=True))
        ads_to_sync = AdCreation.objects.filter(ad_group_creation_id__in=ad_groups_to_sync.values_list("id", flat=True))

        campaign_data = [campaign.get_sync_data() for campaign in campaigns_to_sync]
        ad_group_data = [ad_group.get_sync_data() for ad_group in ad_groups_to_sync]
        ads_data = [ad.get_sync_data() for ad in ads_to_sync]
        data = {
            "campaigns": campaign_data,
            "ad_groups": ad_group_data,
            "ads": ads_data,
        }
        return Response(data)

    def post(self, request, *args, **kwargs):
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
