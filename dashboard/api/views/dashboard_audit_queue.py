from datetime import timedelta
from django.utils import timezone

from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.models import AuditMachine
from audit_tool.models import AuditProcessor
from userprofile.constants import StaticPermissions


class DashboardAuditQueueAPIView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.AUDIT_QUEUE__READ),
    )

    def get(self, request, *args, **kwargs):
        data = {}
        data['active_machine_count'] = AuditMachine.objects.filter(last_seen__gte=timezone.now()-timedelta(minutes=5)).count()
        data['active_audit_count'] = AuditProcessor.objects.filter(source__in=[0,2], temp_stop=False, started__isnull=False, completed__isnull=True).count()
        data['pending_audit_count'] = AuditProcessor.objects.filter(source__in=[0,2], temp_stop=False, started__isnull=True, completed__isnull=True).count()
        if data['active_audit_count'] > 0 or data['pending_audit_count'] > 0:
            data['active_audits'] = self.get_active_audits()
        else:
            data['active_audits'] = []
        return Response(data=data)

    def get_active_audits(self, count=5):
        audits = AuditProcessor.objects.filter(completed__isnull=True, source__in=[0,2], temp_stop=False).order_by("pause", "id")
        res = []
        for audit in audits[:5]:
            res.append(audit.to_dict(get_details=False))
        return res
    