from rest_framework.response import Response

from aw_reporting.utils import get_google_access_token_info
from performiq.api.views.adwords_auth import AdWordsAuthApiView
from oauth.api.views import GoogleOAuthBaseAPIView
from oauth.constants import OAuthType
from oauth.tasks.dv360.sync_dv_records import sync_dv_partners
from performiq.api.views.utils.performiq_permission import PerformIQPermission
from performiq.oauth_utils import load_client_settings


class DV360AuthApiView(GoogleOAuthBaseAPIView):
    permission_classes = (PerformIQPermission,)

    @property
    def oauth_type(self):
        return OAuthType.DV360.value

    @property
    def client_settings(self, *args, **kwargs):
        client_settings = load_client_settings()
        return client_settings

    def handler(self, oauth_account):
        sync_dv_partners.delay(oauth_account_ids=[oauth_account.id], sync_advertisers=True)
        return Response(status=204)
