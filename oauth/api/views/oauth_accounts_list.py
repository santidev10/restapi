from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from utils.views import get_object


class OAuthAccountListPIView(APIView):
    permission_classes = (
        IsAuthenticated,
    )

    def get(self, request, *args, **kwargs):
        user = request.user
        gads_oauth_account = get_object(OAuthAccount, should_raise=False, user=user,
                                        oauth_type=OAuthType.GOOGLE_ADS.value)
        dv360_oauth_account = get_object(OAuthAccount, should_raise=False, user=user,
                                        oauth_type=OAuthType.DV360.value)

        data = {
            "gads": {
                "email": gads_oauth_account.email if gads_oauth_account else None,
            },
            "dv360": {
                "email": dv360_oauth_account.email if dv360_oauth_account else None
            }
        }
        return Response(data)
