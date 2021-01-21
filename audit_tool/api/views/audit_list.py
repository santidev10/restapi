from distutils.util import strtobool
from datetime import timedelta
from django.utils import timezone

from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.models import AuditCategory
from audit_tool.models import AuditMachine
from audit_tool.models import AuditProcessor
from userprofile.constants import StaticPermissions


class AuditListApiView(APIView):
    permission_classes = (
        StaticPermissions()(StaticPermissions.AUDIT_QUEUE),
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
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        source = int(query_params["source"]) if "source" in query_params else None
        try:
            cursor = int(query_params["cursor"]) if "cursor" in query_params else None
            limit = int(query_params["limit"]) if "limit" in query_params else None
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            cursor = None
            limit = None
        if limit and (limit < 15 or limit > 100):
            limit = 25
        if cursor and cursor < 1:
            cursor = 1
        try:
            num_days = int(query_params["num_days"]) if "num_days" in query_params else -1
        except ValueError:
            raise ValidationError("Expected num_days ({}) to be <int> type object. Received object of type {}."
                                  .format(query_params["num_days"], type(query_params["num_days"])))
        active_machine_count = AuditMachine.objects.filter(
            last_seen__gte=timezone.now() - timedelta(minutes=5)).count()
        if audit_id:
            audit_res = AuditProcessor.objects.get(id=int(audit_id))
            return Response({
                'audits': [audit_res.to_dict(get_details=True)],
                'audit_types': AuditProcessor.AUDIT_TYPES,
                'active_machine_count': active_machine_count,
            })
        elif search:
            return Response({
                'audits': AuditProcessor.get(running=False, audit_type=audit_type, search=search, source=source,
                                             cursor=cursor, limit=limit),
                'audit_types': AuditProcessor.AUDIT_TYPES,
                'active_machine_count': active_machine_count,
            })
        else:
            return Response({
                'audits': AuditProcessor.get(running=running, audit_type=audit_type, num_days=num_days, export=export,
                                             source=source, cursor=cursor, limit=limit),
                'audit_types': AuditProcessor.AUDIT_TYPES,
                'youtube_categories': AuditCategory.get_all(iab=False),
                'active_machine_count': active_machine_count,
            })
