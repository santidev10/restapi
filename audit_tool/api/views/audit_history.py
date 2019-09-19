from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditProcessorCache
from utils.permissions import user_has_permission

class AuditHistoryApiView(APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit"),
    )

    def get(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        if audit_id:
            last_time = None
            first_time = None
            try:
                audit = AuditProcessor.objects.get(id=audit_id)
                history = AuditProcessorCache.objects.filter(audit=audit).order_by("id")
                res = {
                    'results': [],
                    'elapsed_time': 'N/A'
                }
                for h in history:
                    if not first_time:
                        first_time = h.created
                    last_time = h.created
                    res['results'].append({
                        'date': h.created.strftime("%m/%d %H:%M"),
                        'count': h.count,
                    })
                try:
                    res['elapsed_time'] = str(last_time - first_time).replace(",", "").split(".")[0]
                except Exception as e:
                    pass
                return Response(res)
            except Exception as e:
                raise ValidationError("invalid audit_id: please check")
