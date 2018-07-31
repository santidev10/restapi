from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.api.serializers.campaign_list_serializer import CampaignListSerializer
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import campaign_type_str, Campaign
from userprofile.models import UserSettingsKey
from utils.permissions import UserHasDashboardPermission
from utils.registry import registry


@demo_view_decorator
class DashboardAccountCreationCampaignsListApiView(APIView):
    permission_classes = (IsAuthenticated, UserHasDashboardPermission)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        user = registry.user
        account_id = AccountCreation.objects.get(id=pk).account_id
        types_hidden = user.get_aw_settings() \
            .get(UserSettingsKey.HIDDEN_CAMPAIGN_TYPES).get(account_id, [])
        types_to_exclude = [campaign_type_str(t) for t in types_hidden]
        queryset = Campaign.objects \
            .filter(
            account__account_creations__id=pk,
            account__account_creations__owner=self.request.user) \
            .exclude(type__in=types_to_exclude) \
            .order_by("name", "id").distinct()
        return queryset

    def get(self, request, pk, **kwargs):
        filters = dict(
            is_deleted=False,
            account__id__in=[],
        )
        user_settings = request.user.get_aw_settings()
        if user_settings.get(UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY):
            filters["account__id__in"] = \
                user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
        try:
            account_creation = AccountCreation.objects.filter(**filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
            campaign_creation_ids = set()
        else:
            campaign_creation_ids = set(
                account_creation.campaign_creations.filter(
                    is_deleted=False
                ).values_list("id", flat=True))
        queryset = self.get_queryset()
        serializer = CampaignListSerializer(
            queryset, many=True, campaign_creation_ids=campaign_creation_ids)
        return Response(serializer.data)
