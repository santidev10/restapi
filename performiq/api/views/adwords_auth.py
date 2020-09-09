from oauth2client import client
from oauth2client.client import HttpAccessTokenRefreshError
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView
from suds import WebFault

from aw_reporting.adwords_api import get_customers
from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.utils import get_google_access_token_info
from performiq.api.serializers.aw_auth_serializer import AWAuthSerializer
from performiq.models.constants import OAuthType
from performiq.models import OAuthAccount


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

    # first step
    def get(self, *args, **kwargs):
        redirect_url = self.request.query_params.get("redirect_url")
        if not redirect_url:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required query param: 'redirect_url'")
            )

        flow = self.get_flow(redirect_url)
        authorize_url = flow.step1_get_authorize_url()
        return Response(dict(authorize_url=authorize_url))

    # second step
    # pylint: disable=too-many-return-statements,too-many-branches,too-many-statements
    def post(self, request, *args, **kwargs):
        # get refresh token
        redirect_url = self.request.query_params.get("redirect_url")
        if not redirect_url:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required query param: 'redirect_url'")
            )

        code = request.data.get("code")
        if not code:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required: 'code'")
            )

        flow = self.get_flow(redirect_url)
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
            try:
                oauth_account = OAuthAccount.objects.get(
                    user=self.request.user,
                    email=token_info["email"]
                )
            except OAuthAccount.DoesNotExist:
                if refresh_token:
                    OAuthAccount.objects.create(
                        oauth_type=OAuthType.GOOGLE_ADS.value,
                        user=self.request.user,
                        email=token_info["email"],
                        token=access_token,
                        refresh_token=refresh_token,
                    )
                else:
                    return Response(
                        data=dict(error=self.lost_perm_error),
                        status=HTTP_400_BAD_REQUEST,
                    )
            else:
                # update token
                if refresh_token and oauth_account.refresh_token != refresh_token:
                    oauth_account.revoked_access = False
                    oauth_account.token = access_token
                    oauth_account.refresh_token = refresh_token
                    oauth_account.save()

        # Get Name of First MCC Account
        try:
            customers = get_customers(
                oauth_account.refresh_token,
                **load_web_app_settings()
            )
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
        else:
            mcc_accounts = list(filter(
                lambda i: i["canManageClients"] and not i["testAccount"],
                customers,
            ))
            if not mcc_accounts:
                return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data=dict(error=self.no_mcc_error)
                )
            primary_account = mcc_accounts[0]
            oauth_account.id = primary_account["customerId"]
            oauth_account.name = primary_account["descriptiveName"]
            oauth_account.save(update_fields=["id", "name"])
            response = AWAuthSerializer(oauth_account).data
            return Response(data=response)
    # pylint: enable=too-many-return-statements,too-many-branches,too-many-statements

    def get_flow(self, redirect_url):
        aw_settings = load_web_app_settings()
        flow = client.OAuth2WebServerFlow(
            client_id=aw_settings.get("client_id"),
            client_secret=aw_settings.get("client_secret"),
            scope=self.scopes,
            user_agent=aw_settings.get("user_agent"),
            redirect_uri=redirect_url,
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
