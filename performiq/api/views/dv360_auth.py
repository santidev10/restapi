from oauth2client import client
from urllib.parse import unquote
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.utils import get_google_access_token_info
from performiq.api.views.adwords_auth import AdWordsAuthApiView
from performiq.api.views.utils.performiq_permission import PerformIQPermission
from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.oauth_utils import load_client_settings
from performiq.tasks.dv360.sync_dv_records import sync_dv_partners

class DV360AuthApiView(AdWordsAuthApiView):
    permission_classes = (PerformIQPermission,)

    scopes = (
        "https://www.googleapis.com/auth/display-video",
        # "https://www.googleapis.com/auth/display-video-media-planning",
        # "https://www.googleapis.com/auth/display-video-user-management",
        "https://www.googleapis.com/auth/doubleclickbidmanager",
        "https://www.googleapis.com/auth/userinfo.email",
    )

    # second step
    # pylint: disable=too-many-return-statements,too-many-branches,too-many-statements
    def post(self, request, *args, **kwargs):
        code = request.data.get("code")
        if not code:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required: 'code'")
            )
        code = unquote(code)

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

            # persist our new data
            oauth_account, _created = OAuthAccount.objects.update_or_create(
                user=self.request.user,
                email=token_info["email"],
                oauth_type=OAuthType.DV360.value,
                defaults={
                    "token": access_token,
                    "refresh_token": refresh_token,
                    "revoked_access": False,
                    "is_enabled": True,
                    "synced": False,
                }
            )

            # get user's  partners and advertisers relations
            sync_dv_partners.delay(oauth_account_ids=[oauth_account.email], sync_advertisers=True)
        return Response(status=204)
