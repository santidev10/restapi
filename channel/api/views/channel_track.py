from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from channel.utils import track_channels
from userprofile.constants import StaticPermissions


class ChannelTrackApiView(APIView, PermissionRequiredMixin):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_VET_ADMIN, StaticPermissions.BUILD__CTL_VET),
    )

    def post(self, request, *args, **kwargs):
        channel_ids = request.data["channel_ids"] if "channel_ids" in request.data else None
        if not channel_ids:
            raise ValidationError("'channel_ids' field is required in POST Body.")
        channel_ids = [channel_id.strip() for channel_id in channel_ids.split(",")]
        try:
            num_tracked = track_channels(channel_ids)
        # pylint: disable=broad-except
        except Exception as e:
            # pylint: enable=broad-except
            raise ValidationError(e)
        return Response(status=HTTP_200_OK, data=f"Added {num_tracked} manually tracked channels.")
