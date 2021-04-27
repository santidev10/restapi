from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from oauth.constants import OAuthData
from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from userprofile.constants import StaticPermissions
from utils.permissions import or_permission_classes
from utils.views import get_object
from utils.datetime import now_in_default_tz


class SegmentGadsScriptAPIView(APIView):
    """
    Provide client with code to paste on Google Ads Scripts and sync with ViewIQ
    """
    permission_classes = (
        or_permission_classes(
            StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_CREATE_VIDEO_LIST),
            StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST),
        ),
    )

    def get(self, request, *args, **kwargs):
        message = "You must OAuth with Google Ads."
        oauth_account = get_object(OAuthAccount, user=request.user, oauth_type=OAuthType.GOOGLE_ADS.value,
                                   message=message, code=HTTP_400_BAD_REQUEST)
        script_fp = "segment/utils/request_create_placements.js"
        with open(script_fp, "r") as file:
            func = file.read()
            code = func.replace("{VIQ_KEY}", str(oauth_account.viq_key))
        response = {
            "code_link": "https://ads.google.com/aw/bulk/scripts/management",
            "code": code,
        }
        # Save timestamp if first time starting OAuth process. Timestamp will be used in segment_gads_oauth_notify_task
        # to prompt users to complete OAuth process
        if oauth_account.data.get(OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP) is None:
            oauth_account.update_data(OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP, str(now_in_default_tz()))
        return Response(response)
