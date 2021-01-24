from distutils.util import strtobool

from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.models import AuditProcessor
from userprofile.constants import StaticPermissions


class AuditPauseApiView(APIView):
    permission_classes = (
        StaticPermissions()(StaticPermissions.AUDIT_QUEUE),
    )

    def post(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        pause = strtobool(query_params["pause"]) if "pause" in query_params else True
        if audit_id:
            try:
                audit = AuditProcessor.objects.get(id=audit_id, completed__isnull=True)
                if audit.temp_stop != pause:
                    audit.temp_stop = pause
                    audit.save(update_fields=['temp_stop'])
                else:
                    raise ValidationError("invalid action")
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                raise ValidationError("invalid audit_id: please verify you are pausing or un-pausing a running audit.")
            return Response(audit.to_dict())
        else:
            return Response({'error': 'must provide audit_id'})
