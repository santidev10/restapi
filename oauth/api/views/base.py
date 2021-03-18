from django.conf import settings
from oauth2client import client
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from oauth.models import OAuthAccount


class BaseOAuthAPIView(APIView):
    lost_perm_error = "You have already provided access to your accounts" \
                      " but we've lost it. Please, visit " \
                      "https://myaccount.google.com/permissions and " \
                      "revoke our application's permission " \
                      "then try again"
    scopes = ()

    def handler(self, *args, **kwargs):
        raise NotImplementedError

    def get_client_settings(self, *args, **kwargs):
        raise NotImplementedError

    def get_flow(self, client_settings):
        # new popup flow, different than redirect flow
        flow = client.OAuth2WebServerFlow(
            client_id=client_settings.get("client_id"),
            client_secret=client_settings.get("client_secret"),
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
