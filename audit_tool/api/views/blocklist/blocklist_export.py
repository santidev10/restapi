from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.tasks.export_blocklist import export_blocklist_task
from userprofile.constants import StaticPermissions


class BlocklistExportAPIView(APIView):
    permission_classes = (StaticPermissions()(StaticPermissions.BLOCKLIST_MANAGER),)

    def get(self, request, *args, **kwargs):
        data_type = kwargs["data_type"]
        export_blocklist_task.delay(request.user.email, data_type)
        response = {
            "message": f"Processing. You will receive an email when your export for the {data_type} blocklist is ready."
        }
        return Response(response)
