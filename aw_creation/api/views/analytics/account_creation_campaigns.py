from django.db.models import Q
from django.http import Http404
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.api.serializers.campaign_list_serializer import CampaignListSerializer
from aw_reporting.models import Campaign, Account


class AnalyticsAccountCreationCampaignsListApiView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request, pk):
        account_creation = self._get_account_creation(pk)
        campaign_creation_ids = set(account_creation.campaign_creations.filter(
            is_deleted=False).values_list("id", flat=True))
        queryset = Campaign.objects \
            .filter(account_id=account_creation.account_id) \
            .order_by("name", "id") \
            .distinct()
        serializer = CampaignListSerializer(queryset, many=True, campaign_creation_ids=campaign_creation_ids)
        return Response(serializer.data)

    def _get_account_creation(self, pk):
        available_account_ids = Account.user_objects(self.request.user).values_list("id", flat=True)
        try:
            return AccountCreation.objects.filter(
                Q(is_deleted=False) & Q(owner=self.request.user) | Q(account_id__in=available_account_ids)).get(id=pk)
        except AccountCreation.DoesNotExist:
            raise Http404
