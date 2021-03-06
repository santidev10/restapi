from django.http import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.api.serializers.campaign_list_serializer import CampaignListSerializer
from aw_reporting.models import Campaign
from aw_reporting.models import campaign_type_str
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from userprofile.constants import UserSettingsKey
from userprofile.constants import StaticPermissions


class DashboardAccountCreationCampaignsListApiView(APIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MANAGED_SERVICE),)

    def get_queryset(self, account_id):
        types_hidden = self.request.user.get_aw_settings().get(
            UserSettingsKey.HIDDEN_CAMPAIGN_TYPES).get(str(account_id), [])
        types_to_exclude = [campaign_type_str(c_type) for c_type in types_hidden]
        # Ignore demo account from aw_settings
        if DEMO_ACCOUNT_ID in types_to_exclude:
            types_to_exclude.remove(DEMO_ACCOUNT_ID)
        queryset = Campaign.objects.filter(account_id=account_id).exclude(
            type__in=types_to_exclude).order_by("name", "id").distinct()
        return queryset

    def get(self, request, pk):
        account_creation = self._get_account_creation(pk)
        campaign_creation_ids = set(account_creation.campaign_creations.filter(
            is_deleted=False).values_list("id", flat=True))
        queryset = self.get_queryset(account_creation.account_id)
        serializer = CampaignListSerializer(
            queryset, many=True, campaign_creation_ids=campaign_creation_ids)
        return Response(serializer.data)

    def _get_account_creation(self, pk):
        if not pk.isnumeric():
            raise Http404
        filters = {"is_deleted": False}
        if not self.request.user.has_permission(StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS):
            visible_accounts = self.request.user.get_visible_accounts_list()
            filters["account_id__in"] = visible_accounts
        try:
            return AccountCreation.objects.filter(**filters).get(id=pk)
        except AccountCreation.DoesNotExist:
            raise Http404
