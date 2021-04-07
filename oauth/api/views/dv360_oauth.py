from rest_framework.response import Response

from .base_google_oauth import BaseGoogleOAuthAPIView
from oauth.constants import OAuthType
from oauth.tasks.dv360.sync_dv_records import sync_dv_partners


class DV360AuthApiView(BaseGoogleOAuthAPIView):

    @property
    def oauth_type(self):
        return OAuthType.DV360.value

    def handler(self, oauth_account):
        sync_dv_partners.delay(oauth_account_ids=[oauth_account.id], sync_advertisers=True)
        return Response(status=204)
