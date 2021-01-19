from rest_framework_csv.renderers import CSVStreamingRenderer

from brand_safety.api.serializers.bad_channel_serializer import BadChannelSerializer
from brand_safety.models import BadChannel
from userprofile.constants import StaticPermissions
from utils.api.file_list_api_view import FileListApiView
from utils.permissions import has_static_permission


class BadChannelCSVRendered(CSVStreamingRenderer):
    header = [
        "id",
        "youtube_id",
        "title",
        "category",
        "thumbnail_url",
        "reason",
    ]
    labels = {
        "category": "Category",
        "id": "Id",
        "reason": "Reason",
        "thumbnail_url": "Thumbnail URL",
        "title": "Title",
        "youtube_id": "Youtube ID",
    }


class BadChannelExportApiView(FileListApiView):
    permission_classes = (has_static_permission(StaticPermissions.ADMIN),)
    serializer_class = BadChannelSerializer
    renderer_classes = (BadChannelCSVRendered,)
    queryset = BadChannel.objects.all().order_by("title")
    filename = "Bad Channels.csv"
