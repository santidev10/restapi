from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.tasks.export_blocklist import export_blocklist_task


class BlocklistExportAPIView(APIView):
    permission_classes = (IsAdminUser,)

    def get(self, request, *args, **kwargs):
        data_type = kwargs["data_type"]
        export_blocklist_task.delay(request.user.email, data_type)
        return Response()
