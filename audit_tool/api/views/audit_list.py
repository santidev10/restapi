from distutils.util import strtobool
from rest_framework.views import APIView
from rest_framework.response import Response
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditCategory
from rest_framework.exceptions import ValidationError
from utils.permissions import user_has_permission

class AuditListApiView(APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit"),
    )

    def get(self, request):
        query_params = request.query_params
        running = query_params["running"] if "running" in query_params else None
        export = None
        if running and running.lower() == "export":
            running = None
            export = True
        elif running and running in ['true', 'false', '0', '1']:
            running = strtobool(running.lower())
        audit_type = query_params["audit_type"] if "audit_type" in query_params else None
        search = query_params["search"] if "search" in query_params else None
        source = int(query_params["source"]) if "source" in query_params else 0
        cursor = int(query_params["cursor"]) if "cursor" in query_params else None
        limit = int(query_params["limit"]) if "limit" in query_params else None
        if limit and (limit < 15 or limit > 100):
            limit = 25
        if cursor and cursor < 1:
            cursor = 1
        try:
            num_days = int(query_params["num_days"]) if "num_days" in query_params else -1
        except ValueError:
            raise ValidationError("Expected num_days ({}) to be <int> type object. Received object of type {}."
                                  .format(query_params["num_days"], type(query_params["num_days"])))
        if search:
            return Response({
                'audits': AuditProcessor.get(running=False, audit_type=audit_type, search=search, source=source, cursor=cursor, limit=limit),
                'audit_types': AuditProcessor.AUDIT_TYPES,
            })
        else:
            return Response({
                'audits': AuditProcessor.get(running=running, audit_type=audit_type, num_days=num_days, export=export, source=source, cursor=cursor, limit=limit),
                'audit_types': AuditProcessor.AUDIT_TYPES,
                'youtube_categories': AuditCategory.get_all(iab=False),
            })
