from googleapiclient.errors import HttpError
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from .base_google_oauth import BaseGoogleOAuthAPIView
from oauth.constants import OAuthType
from oauth.tasks.dv360.sync_dv_records import sync_dv_partners
from oauth.utils.dv360 import get_discovery_resource
from oauth.utils.dv360 import request_partners
from oauth.utils.dv360 import load_credentials


class DV360AuthApiView(BaseGoogleOAuthAPIView):

    @property
    def oauth_type(self):
        return OAuthType.DV360.value

    def handler(self, oauth_account):
        # Check if user has any dv360 access
        credentials = load_credentials(oauth_account)
        resource = get_discovery_resource(credentials)
        try:
            request_partners(resource)
        except HttpError:
            return Response(status=HTTP_400_BAD_REQUEST, data="You do not have access to any DV360 resources.")
        sync_dv_partners.delay(oauth_account_ids=[oauth_account.id], sync_advertisers=True)
        return Response(status=204)
