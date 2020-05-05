from django.db.models import Case
from django.db.models import When
from django.db.models import IntegerField
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from django.db.models import F
from django.db.models import Q


class AccountSyncAPIView(APIView):
    CAMPAIGN_FIELDS = ("id", "name", "bid_strategy_type", "budget", "type", "start", "end")
    AD_GROUP_FIELDS = ("id", "campaign_name", "campaign_type", "name", "max_rate", "status", "source")

    def get(self, request, *args, **kwargs):
        """ Get data to sync on Google Ads """
        account_id = kwargs["account_id"]
        account_creation = AccountCreation.objects.get(account_id=account_id)

        campaign_creations = CampaignCreation.objects.filter(account_creation=account_creation)\
            .filter(Q(sync_at__lte=F("updated_at")) | Q(sync_at=None))\
            .values(*self.CAMPAIGN_FIELDS)

        ad_group_creations = AdGroupCreation.objects.filter(campaign_creation__account_creation=account_creation)\
            .filter(Q(sync_at__lte=F("updated_at")) | Q(sync_at=None))\
            .annotate(
                campaign_name=F("campaign_creation__name"),
                campaign_type=F("campaign_creation__type"),
                source=Case(
                    When(sync_at=None, then=F("ad_group_id")),
                    default=0,
                    output_field=IntegerField()
                )
            )\
            .values(*self.AD_GROUP_FIELDS)
        sync_data = {
            "campaigns": campaign_creations,
            "ad_groups": ad_group_creations,
        }
        return Response(sync_data)

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
