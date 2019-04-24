import logging

from django.http import Http404
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.api.serializers.analytics.account_creation_details_serializer import \
    AnalyticsAccountCreationDetailsSerializer
from aw_creation.models import AccountCreation

logger = logging.getLogger(__name__)


class AnalyticsAccountCreationDetailsAPIView(APIView):
    serializer_class = AnalyticsAccountCreationDetailsSerializer
    permission_classes = (IsAuthenticated,)

    def post(self, request, pk, **_):
        account_creation = self._get_account_creation(request, pk)
        data = AnalyticsAccountCreationDetailsSerializer(account_creation, context={"request": request}).data
        return Response(data=data)

    def _get_account_creation(self, request, pk):
        user = request.user
        try:
            return AccountCreation.objects.user_related(user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404
