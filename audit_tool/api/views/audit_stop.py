from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from audit_tool.models import AuditProcessor
from django.utils import timezone
from utils.permissions import user_has_permission

class AuditStopApiView(APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit"),
    )

    def post(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        if audit_id:
            try:
                audit = AuditProcessor.objects.get(id=audit_id, completed__isnull=True)
                audit.params['stopped'] = True
                audit.completed = timezone.now()
                audit.pause = 0
                audit.save(update_fields=['completed', 'params', 'pause'])
            except Exception as e:
                raise ValidationError("invalid audit_id: please verify you are stopping a running audit.")
            return Response(audit.to_dict())
        else:
            return Response({'error': 'must provide audit_id'})