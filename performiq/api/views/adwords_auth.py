from django.conf import settings
from googleads.errors import GoogleAdsServerFault
from oauth2client import client
from oauth2client.client import HttpAccessTokenRefreshError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView
from suds import WebFault

from performiq.api.serializers.aw_auth_serializer import AWAuthSerializer
from performiq.models.constants import OAuthType
from performiq.models import OAuthAccount
from performiq.oauth_utils import get_google_access_token_info
from performiq.oauth_utils import load_client_settings
from performiq.tasks.update_campaigns import update_campaigns_task
from performiq.utils.adwords_report import get_accounts


class AdWordsAuthApiView(APIView):
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

        flow = self.get_flow()
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

        # Get Name of First MCC Account
        try:
            mcc_accounts, cid_accounts = get_accounts(oauth_account.refresh_token)
        except WebFault as e:
            fault_string = e.fault.faultstring
            if "AuthenticationError.NOT_ADS_USER" in fault_string:
                fault_string = "AdWords account does not exist"
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error=fault_string))
        except HttpAccessTokenRefreshError as e:
            ex_token_error = "Token has been expired or revoked"
            if ex_token_error in str(e):
                return Response(status=HTTP_400_BAD_REQUEST,
                                data=dict(error=ex_token_error))
        except GoogleAdsServerFault as e:
            for error in e.errors:
                authentication_error = "AuthenticationError.CUSTOMER_NOT_FOUND"
                if authentication_error in error.errorString:
                    error_message = "Authentication error. Are you sure you have access to google ads?"
                    return Response(status=HTTP_400_BAD_REQUEST,
                                    data=dict(error=error_message))
        else:
            if mcc_accounts:
                first = mcc_accounts[0]
                oauth_account.name = first["descriptiveName"]
                oauth_account.save(update_fields=["name"])

            if mcc_accounts or cid_accounts:
                response = AWAuthSerializer(oauth_account).data
                status = HTTP_200_OK
            else:
                response = "You have no accounts to sync."
                status = HTTP_400_BAD_REQUEST
            update_campaigns_task.delay(oauth_account.id)
            return Response(data=response, status=status)
    # pylint: enable=too-many-return-statements,too-many-branches,too-many-statements

    def get_flow(self):
        aw_settings = load_client_settings()
        # new popup flow, different than redirect flow
        flow = client.OAuth2WebServerFlow(
            client_id=aw_settings.get("client_id"),
            client_secret=aw_settings.get("client_secret"),
            scope=self.scopes,
            access_type="offline",
            response_type="code",
            prompt="consent",  # SEE https://github.com/googleapis/google-api-python-client/issues/213
            redirect_uri=settings.GOOGLE_APP_OAUTH2_REDIRECT_URL,
            origin=settings.GOOGLE_APP_OAUTH2_ORIGIN
        )
        return flow

    @staticmethod
    def delete(request, email, **_):
        try:
            oauth_account = OAuthAccount.objects.get(
                user=request.user,
                email=email
            )
            oauth_account.delete()
        except OAuthAccount.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        return Response(data=f"Deleted OAuth for email: {email}.")
