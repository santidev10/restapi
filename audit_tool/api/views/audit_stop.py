from django.utils import timezone
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.models import AuditProcessor
from userprofile.constants import StaticPermissions


class AuditStopApiView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.AUDIT_QUEUE),
    )

    def post(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        if audit_id:
            try:
                audit = AuditProcessor.objects.get(id=audit_id, completed__isnull=True)
                if audit.source == 2:
                    return Response({'error': 'can not stop a CTL audit.'})
                audit.params['stopped'] = True
                audit.completed = timezone.now()
                audit.pause = 0
                if audit.params.get('audit_type_original'):
                    audit.audit_type = audit.params.get('audit_type_original')
                audit.save(update_fields=['completed', 'params', 'pause', 'audit_type'])
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                raise ValidationError("invalid audit_id: please verify you are stopping a running audit.")
            return Response(audit.to_dict())
        else:
            return Response({'error': 'must provide audit_id'})
