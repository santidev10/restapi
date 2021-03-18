from oauth2client import client
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from .base import BaseOAuthAPIView
from oauth.models import OAuthAccount
from oauth.constants import OAuthType
from oauth.utils.client import get_google_access_token_info


class BaseGoogleAuthApiView(BaseOAuthAPIView):
    """
    API View for Granting AdWords OAuth Access to PerformIQ
    GET method gives a URL to go and grant access to our app
    then send the code you will get in the query in POST request

    POST body example:
    {"code": "<INSERT YOUR CODE HERE>"}

    success POST response example:
    {"email": "your@email.com",
    "mcc_accounts": [{"id": 1234, "name": "Test Acc", "currency_code": "UAH",
     "timezone": "Ukraine/Kiev"}]
    }
    """

    scopes = (
        "https://www.googleapis.com/auth/adwords",
        "https://www.googleapis.com/auth/userinfo.email",
    )
    lost_perm_error = "You have already provided access to your accounts" \
                      " but we've lost it. Please, visit " \
                      "https://myaccount.google.com/permissions and " \
                      "revoke our application's permission " \
                      "then try again"
    no_mcc_error = "MCC account wasn't found. Please check that you " \
                   "really have access to at least one."

    # second step
    # pylint: disable=too-many-return-statements,too-many-branches,too-many-statements
    def post(self, request, *args, **kwargs):
        code = request.data.get("code")
        if not code:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required: 'code'")
            )

        flow = self.get_flow(self.get_client_settings())
        try:
            credential = flow.step2_exchange(code)
        except client.FlowExchangeError as e:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Authentication has failed: %s" % e)
            )
        else:
            token_info = get_google_access_token_info(credential.access_token)
            if "email" not in token_info:
                return Response(status=HTTP_400_BAD_REQUEST,
                                data=token_info)
            access_token = credential.access_token
            refresh_token = credential.refresh_token
            if not refresh_token:
                return Response(
                    data=dict(error=self.lost_perm_error),
                    status=HTTP_400_BAD_REQUEST,
                )

            oauth_account, _created = OAuthAccount.objects.update_or_create(
                user=self.request.user,
                email=token_info["email"],
                oauth_type=OAuthType.GOOGLE_ADS.value,
                defaults={
                    "token": access_token,
                    "refresh_token": refresh_token,
                    "revoked_access": False,
                    "is_enabled": True,
                    "synced": False,
                }
            )
        response = self.handler(oauth_account)
        return response
