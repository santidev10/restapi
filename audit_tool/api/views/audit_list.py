from copy import deepcopy

from rest_framework.views import APIView
from rest_framework.response import Response
from audit_tool.models import AuditProcessor

class AuditListApiView(APIView):
    def get(self, request):
        query_params = deepcopy(request.query_params)
        running = query_params["running"] if "running" in query_params else None
        if running:
            running = self.convert_boolean(running)
        audit_type = query_params["audit_type"] if "audit_type" in query_params else None

        return Response(AuditProcessor.get(running=running, audit_type=audit_type))

    @staticmethod
    def convert_boolean(param):
        if param.lower() == "true" or param == 1:
            return True
        if param.lower() == "false" or param == 0:
            return False
