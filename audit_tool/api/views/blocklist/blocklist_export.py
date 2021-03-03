from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.tasks.export_blocklist import export_blocklist_task
from userprofile.constants import StaticPermissions


class ChannelBlockListExportAPIView(APIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.BLOCKLIST_MANAGER__EXPORT_CHANNEL),)

    def get(self, request, *args, **kwargs):
        data_type = "channel"
        return Response(export_blocklist(self.request.user.email, data_type))


class VideoBlockListExportAPIView(APIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.BLOCKLIST_MANAGER__EXPORT_VIDEO),)

    def get(self, request, *args, **kwargs):
        data_type = "video"
        return Response(export_blocklist(self.request.user.email, data_type))


def export_blocklist(email, data_type):
    export_blocklist_task.delay(email, data_type)
    response = {
        "message": f"Processing. You will receive an email when your export for the {data_type} blocklist is ready."
    }
    return response