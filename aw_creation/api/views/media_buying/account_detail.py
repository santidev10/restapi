from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_creation.api.serializers.analytics.account_creation_details_serializer import \
    AnalyticsAccountCreationDetailsSerializer
from aw_creation.api.serializers.media_buying.account_serializer import AccountMediaBuyingSerializer
from aw_creation.models import AccountCreation


class AccountDetailAPIView(APIView):
    """
   GET: Retrieve account details

   """

    serializer_class = AnalyticsAccountCreationDetailsSerializer

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        account_creation = self._get_account_creation(request, pk)
        data = AccountMediaBuyingSerializer(account_creation, context=dict(request=request)).data
        return Response(data=data)

    def _get_account_creation(self, request, pk):
        user = request.user
        try:
            return AccountCreation.objects.user_related(user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404
