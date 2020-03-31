import csv
from io import StringIO

from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.response import Response
from rest_framework.views import APIView
from utils.permissions import user_has_permission
from utils.permissions import or_permission_classes
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_200_OK
from channel.utils import track_channels


class ChannelTrackApiView(APIView, PermissionRequiredMixin):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.vet_audit"),
            user_has_permission("userprofile.vet_audit_admin")
        ),
    )

    def post(self, request, *args, **kwargs):
        channel_ids = request.data['channel_ids'] if "channel_ids" in request.data else None
        if not channel_ids:
            raise ValidationError("'channel_ids' field is required in POST Body.")
        channel_ids = channel_ids.split(",")
        try:
            num_tracked = track_channels(channel_ids)
        except Exception as e:
            raise ValidationError(e)
        return Response(status=HTTP_200_OK, data=f"Added {num_tracked} manually tracked channels.")
