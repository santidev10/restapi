from django.db.models import Case
from django.db.models import Exists
from django.db.models import F
from django.db.models import IntegerField
from django.db.models import OuterRef
from django.db.models import Q
from django.db.models import When
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_creation.models import AccountCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.models import Account


class AccountSyncAPIView(APIView):
    NEW_CAMPAIGN_FIELDS = ("id", "name", "bid_strategy_type", "budget", "type", "start", "end", "target_cpa", "sub_type")
    UPDATE_CAMPAIGN_FIELDS = ("id", "name", "budget")
    AD_GROUP_FIELDS = ("id", "campaign_name", "campaign_type", "name", "max_rate", "status", "source")
    AD_GROUP_TARGETING_FIELDS = ("status", "is_negative", "type__name")

    permission_classes = ()

    def get(self, request, *args, **kwargs):
        """ Get data to sync on Google Ads """
        account_id = kwargs["account_id"]
        if "changed" in request.query_params:
            data = self._get_changed_accounts(account_id)
        else:
            data = self._get_account_sync_data(account_id)
        return Response(data)

    def _get_changed_accounts(self, mcc_id):
        creation_exists_subquery = CampaignCreation.objects.filter(
            Q(sync_at__lte=F("updated_at")) | Q(sync_at=None),
            account_creation__account_id=OuterRef("pk"),
        )
        changed_accounts = Account.objects\
            .filter(managers__id=mcc_id)\
            .annotate(sync_pending=Exists(creation_exists_subquery))\
            .filter(Q(sync_pending=True))\
            .values_list("id", flat=True)
        return changed_accounts

    def _get_account_sync_data(self, account_id):
        account_creation = AccountCreation.objects.get(account_id=account_id)

        # Only provide relevant fields to Google ads depending if new or not
        base_query = CampaignCreation.objects.filter(account_creation=account_creation) \
            .filter(Q(sync_at__lte=F("updated_at")) | Q(sync_at=None))
        to_update = base_query.filter(Q(campaign__isnull=False)).values(*self.UPDATE_CAMPAIGN_FIELDS)
        to_create = base_query.exclude(Q(campaign__isnull=False)).values(*self.NEW_CAMPAIGN_FIELDS)

        # If an ad group has never been synced, set source to copy targeting settings
        # from existing ad group on Google ads
        ad_group_creations = AdGroupCreation.objects.filter(campaign_creation__account_creation=account_creation) \
            .filter(Q(sync_at__lte=F("updated_at")) | Q(sync_at=None)) \
            .annotate(
                campaign_name=F("campaign_creation__name"),
                campaign_type=F("campaign_creation__type"),
                source=Case(
                    When(name__contains="#", then=F("ad_group_id")),
                    default=0,
                    output_field=IntegerField()
                )
            ) \
            .values(*self.AD_GROUP_FIELDS)
        sync_data = {
            "campaigns": list(to_create) + list(to_update),
            "ad_groups": ad_group_creations,
        }
        return sync_data

    def patch(self, request, *args, **kwargs):
        """ Set sync times for creation items """
        now = timezone.now()
        data = request.data
        account_id = kwargs["account_id"]
        account_creation = AccountCreation.objects.get(account_id=account_id)
        campaign_ids = data["campaign_ids"]
        ad_group_ids = data["ad_group_ids"]
        CampaignCreation.objects.filter(account_creation=account_creation, id__in=campaign_ids)\
            .update(sync_at=now)
        AdGroupCreation.objects.filter(id__in=ad_group_ids)\
            .update(sync_at=now)
        return Response(data)
