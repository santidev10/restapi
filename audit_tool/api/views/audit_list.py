from distutils.util import strtobool
from rest_framework.views import APIView
from rest_framework.response import Response
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditCategory
from rest_framework.exceptions import ValidationError

class AuditListApiView(APIView):
    def get(self, request):
        query_params = request.query_params
        running = query_params["running"] if "running" in query_params else None
        if running:
            running = strtobool(running.lower())
        audit_type = query_params["audit_type"] if "audit_type" in query_params else None
        try:
            num_days = int(query_params["num_days"]) if "num_days" in query_params else 60
        except ValueError:
            raise ValidationError("Expected num_days ({}) to be <int> type object. Received object of type {}."
                                  .format(query_params["num_days"], type(query_params["num_days"])))
        return Response({
            'audits': AuditProcessor.get(running=running, audit_type=audit_type, num_days=num_days),
            'audit_types': AuditProcessor.AUDIT_TYPES,
            'youtube_categories': AuditCategory.get_all(),
        })
