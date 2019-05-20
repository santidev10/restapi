from distutils.util import strtobool

from rest_framework.views import APIView
from rest_framework.response import Response
from audit_tool.models import AuditProcessor


class AuditSaveApiView(APIView):
    def get(self, request):
        query_params = request.query_params
        running = query_params["running"] if "running" in query_params else None
        if running:
            running = strtobool(running.lower())
        audit_type = query_params["audit_type"] if "audit_type" in query_params else None

        return Response({
            'audits': AuditProcessor.get(running=running, audit_type=audit_type),
            'audit_types': AuditProcessor.AUDIT_TYPES
        })
