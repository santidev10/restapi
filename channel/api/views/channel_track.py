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
        channel_ids_file = request.data['channel_ids_file'] if "channel_ids_file" in request.data else None
        if not channel_ids_file:
            raise ValidationError("channel_ids_file is required.")
        channel_ids_file_split = channel_ids_file.name.split(".")
        if len(channel_ids_file_split) != 2:
            raise ValidationError(f"Invalid channel_ids_file. Expected CSV file. Received {channel_ids_file.name}.")
        channel_ids_file_type = channel_ids_file_split[1]
        if channel_ids_file_type != "csv":
            raise ValidationError(f"Invalid channel_ids_file type. Expected CSV. Received '{channel_ids_file_type}'.")
        file = channel_ids_file.read().decode('utf-8-sig')
        io_string = StringIO(file)
        reader = csv.reader(io_string, delimiter=";", quotechar="|")
        new_channel_ids = []
        for row in reader:
            new_channel_ids.append(row[0])
        try:
            num_tracked = track_channels(new_channel_ids)
        except Exception as e:
            raise ValidationError(e)
        return Response(status=HTTP_200_OK, data=f"Added {num_tracked} manually tracked channels.")
