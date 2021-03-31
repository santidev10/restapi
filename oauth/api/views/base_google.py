import abc

from django.conf import settings
from oauth2client import client
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView
from rest_framework.status import HTTP_404_NOT_FOUND

from oauth.models import OAuthAccount
from oauth.constants import OAuthType
from oauth.utils.client import get_google_access_token_info


class GoogleOAuthBaseAPIView(APIView):
    """
    Base OAuth API view for Google APIs
    """

    permission_classes = None
    lost_perm_error = "You have already provided access to your accounts" \
        " but we've lost it. Please, visit https://myaccount.google.com/permissions and" \
        " revoke our application's permission then try again"

    SCOPES = {
        OAuthType.GOOGLE_ADS.value: (
            "https://www.googleapis.com/auth/adwords",
            "https://www.googleapis.com/auth/userinfo.email",
        ),
        OAuthType.DV360.value: (
            "https://www.googleapis.com/auth/display-video",
            "https://www.googleapis.com/auth/doubleclickbidmanager",
            "https://www.googleapis.com/auth/userinfo.email",
        ),
    }

    @property
    def oauth_type(self, *_, **__) -> int:
        """ Return an OAuthType enum value """
        raise NotImplementedError

    @property
    def client_settings(self, *args, **kwargs) -> dict:
        """
        Return the appropriate OAuth client settings for the app.
        Should contain keys: user_agent, client_id, client_secret, developer_token
        """
        raise NotImplementedError

    def handler(self, oauth_account: OAuthAccount) -> Response:
        """
        Method that is called if OAuth is successful in cls.post method.
        This should contain logic for the current use case to complete oauth process and must return a Response object
        :param oauth_account: OAuthAccount that is created or updated in cls.create_oauth_account method
        """
        raise NotImplementedError

    @property
    def scopes(self):
        """ Return scopes required for API access. Override this property if different scopes are required
        other than in cls.SCOPES, """
        return self.SCOPES[self.oauth_type]

    def post(self, request, *args, **kwargs) -> Response:
        """
        Handle OAuth process.
        cls.handler is called if OAuth is successful.
        """
        code = request.data.get("code")
        if not code:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required: 'code'")
            )
        flow = client.OAuth2WebServerFlow(
            client_id=self.client_settings.get("client_id"),
            client_secret=self.client_settings.get("client_secret"),
            scope=self.scopes,
            access_type="offline",
            response_type="code",
            prompt="consent",  # SEE https://github.com/googleapis/google-api-python-client/issues/213
            redirect_uri=settings.GOOGLE_APP_OAUTH2_REDIRECT_URL,
            origin=settings.GOOGLE_APP_OAUTH2_ORIGIN
        )
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
                return Response(status=HTTP_400_BAD_REQUEST, data=token_info)
            access_token = credential.access_token
            refresh_token = credential.refresh_token
            if not refresh_token:
                return Response(data=dict(error=self.lost_perm_error), status=HTTP_400_BAD_REQUEST)
            oauth_account = self.create_oauth_account(token_info["email"], access_token, refresh_token)
            response = self.handler(oauth_account)
            return response

    def create_oauth_account(self, email, access_token, refresh_token):
        oauth_account, _ = OAuthAccount.objects.update_or_create(
            user=self.request.user,
            email=email,
            oauth_type=self.oauth_type,
            defaults={
                "token": access_token,
                "refresh_token": refresh_token,
                "revoked_access": False,
                "is_enabled": True,
                "synced": False,
            }
        )
        return oauth_account

    @staticmethod
    def delete_oauth_account(request, email):
        try:
            oauth_account = OAuthAccount.objects.get(
                user=request.user,
                email=email,
            )
            oauth_account.delete()
        except OAuthAccount.DoesNotExist:
            response = Response(status=HTTP_404_NOT_FOUND)
        else:
            response = Response(data=f"Deleted OAuth for email: {email}.")
        return response
