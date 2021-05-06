from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from oauth.api.serializers import OAuthAccountSerializer
from oauth.constants import OAuthType
from oauth.models import OAuthAccount


class OAuthAccountListUpdateAPIView(APIView):
    permission_classes = (
        IsAuthenticated,
    )

    def get(self, request, *args, **kwargs):
        user = request.user
        gads_oauth_account = OAuthAccount.get_enabled(user=user, oauth_type=OAuthType.GOOGLE_ADS.value).first()
        dv360_oauth_account = OAuthAccount.get_enabled(user=user, oauth_type=OAuthType.DV360.value).first()
        data = {
            "gads": OAuthAccountSerializer(gads_oauth_account).data if gads_oauth_account else None,
            "dv360": OAuthAccountSerializer(dv360_oauth_account).data if dv360_oauth_account else None,
        }
        return Response(data)

    def patch(self, request, *args, **kwargs):
        oauth_accounts = OAuthAccount.objects.filter(user=request.user, oauth_type=request.query_params.get("oauth_type"))
        for oauth in oauth_accounts:
            serializer = OAuthAccountSerializer(oauth, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
        return Response(request.data)
