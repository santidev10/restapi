from datetime import timedelta
from django.utils import timezone

from rest_framework.response import Response
from rest_framework.views import APIView
from utils.permissions import user_has_permission

from audit_tool.models import AuditMachine
from audit_tool.models import AuditProcessor

class DashboardAuditQueueAPIView(APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit"),
    )

    def get(self, request, *args, **kwargs):
        data = {}
        data['active_machine_count'] = AuditMachine.objects.filter(last_seen__gte=timezone.now()-timedelta(minutes=5)).count()
        data['active_audit_count'] = AuditProcessor.objects.filter(source=0, temp_stop=False, started__isnull=False, completed__isnull=True).count()
        data['pending_audit_count'] = AuditProcessor.objects.filter(source=0, temp_stop=False, started__isnull=True).count()
        data['active_audits'] = self.get_active_audits()
        return Response(data=data)

    def get_active_audits(self, count=5):
        audits = AuditProcessor.objects.filter(completed__isnull=True, source=0, temp_stop=False).order_by("pause", "id")
        res = []
        for audit in audits:
            res.append(audit.to_dict(get_details=False))
        return res
    