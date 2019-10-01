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
            first_count = 0
            last_count = 0
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
            previous = None
            every_other = 1
            if history.count() > 1440:
                every_other = 5
            position = 0
            for h in history:
                if not first_time:
                    first_time = h.created
                    first_count = h.count
                last_time = h.created
                last_count = h.count
                rate = h.count - previous if previous else None
                if position % every_other == 0:
                    res['results'].append({
                        'date': h.created.astimezone(pytz.timezone('America/Los_Angeles')).strftime("%m/%d %I:%M %p"),
                        'count': h.count,
                        'rate': rate,
                    })
                previous = h.count
                position += 1
            try:
                res['elapsed_time'] = str(last_time - first_time).replace(",", "").split(".")[0]
            except Exception as e:
                pass
            try:
                diff = (last_time - first_time)
                minutes = (diff.total_seconds() / 60)
                res['rate_average'] = (last_count - first_count) / minutes
            except Exception as e:
                pass
            return Response(res)
