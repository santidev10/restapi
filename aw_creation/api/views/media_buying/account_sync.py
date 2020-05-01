from django.db.models import F
from django.db.models import Value
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
from aw_reporting.models import Account
from aw_reporting.models import Ad
from django.db.models import F
from django.db.models import Q


class AccountSyncAPIView(APIView):
    CAMPAIGN_FIELDS = ("id", "name", "bid_strategy_type", "budget", "type", "start", "end")
    AD_GROUP_FIELDS = ("id", "campaign_name", "campaign_type", "name", "max_rate", "status", "source")

    def get(self, request, *args, **kwargs):
        """ Get data to sync on Google Ads """
        from django.db.models import Subquery
        from django.db.models import OuterRef
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

        campaign_creations = Account.objects.get(id=account_id).account_creation.campaign_creations.all().values(*self.CAMPAIGN_FIELDS)
        sync_data = []

        for campaign_creation in campaign_creations:
            ad_groups = AdGroupCreation.objects.filter(campaign_creation_id=campaign_creation["id"])\
                .annotate(campaign_name=F("campaign_creation__name"))\
                .values(*self.AD_GROUP_FIELDS)
            try:
                ag_id = ad_groups[0]["ad_group_id"]
                ad_id = Ad.objects.filter(ad_group_id=ag_id).first().id
                ad_id_key = [ag_id, ad_id]
            except (AttributeError, IndexError):
                ad_id_key = []
            data = {
                "campaign": campaign_creation,
                "ad_groups": {
                    ag_creation["ad_group_id"]: ag_creation
                    for ag_creation in ad_groups
                },
                "ad_id": ad_id_key
            }
            sync_data.append(data)
            [
                {
                    "campaign": "",
                    "source": agid,
                    "status": None
                }
            ]
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
