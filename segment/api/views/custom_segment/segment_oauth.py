from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError

from performiq.api.serializers import AdGroupSerializer
from performiq.api.serializers import OAuthAccountSerializer
from segment.models import CustomSegment
from segment.models.constants import Params
from performiq.models import Account
from performiq.models import OAuthAccount
from utils.views import get_object


class SegmentOAuthAPIView(APIView):
    def get(self, request, *args, **kwargs):
        # get oauth cid or adgroup ids
        oauth_account = get_object(OAuthAccount, user=request.user, message="OAuth account not found. Please OAuth.")
        cid = request.query_params.get("cid")
        if cid:
            qs = AdGroup.objects.filter(campaign__account=cid)
        else:
            qs = oauth_account.gads_accounts.all()
        data = self._get_data(qs)
        return Response(dict(data=data))

    def _get_data(self, qs):
        if isinstance(qs.model, OAuthAccount):
            serializer = OAuthAccountSerializer
        else:
            serializer = AdGroupSerializer
        data = serializer(qs, many=True).data
        return data
