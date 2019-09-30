from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditProcessorCache
from utils.permissions import user_has_permission
from datetime import timedelta
from django.utils import timezone
import pytz

class AuditHistoryApiView(APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit"),
    )

    def get(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        hours = int(query_params["hours"]) if "hours" in query_params else None
        if audit_id:
            last_time = None
            first_time = None
            try:
                audit = AuditProcessor.objects.get(id=audit_id)
            except Exception as e:
                raise ValidationError("invalid audit_id: please check")
            history = AuditProcessorCache.objects.filter(audit=audit)
            if hours:
                history = history.filter(created__gt=timezone.now() - timedelta(hours=hours))
            history = history.order_by("id")
            res = {
                'results': [],
                'elapsed_time': 'N/A'
            }
            avg_sum = 0
            avg_count = 0
            previous = None
            for h in history:
                if not first_time:
                    first_time = h.created
                last_time = h.created
                rate = h.count - previous if previous else None
                if rate:
                    avg_sum+=rate
                    avg_count+=1
                res['results'].append({
                    'date': h.created.astimezone(pytz.timezone('America/Los_Angeles')).strftime("%m/%d %I:%M %p"),
                    'count': h.count,
                    'rate': rate,
                })
                previous = h.count
            try:
                res['rate_average'] = avg_sum / avg_count if avg_count > 0 else None
            except Exception as e:
                pass
            try:
                res['elapsed_time'] = str(last_time - first_time).replace(",", "").split(".")[0]
            except Exception as e:
                pass
            return Response(res)
