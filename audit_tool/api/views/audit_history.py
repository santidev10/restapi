from datetime import timedelta

import pytz
from django.utils import timezone
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.models import AuditProcessor
from audit_tool.models import AuditProcessorCache
from userprofile.constants import StaticPermissions


class AuditHistoryApiView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.AUDIT_QUEUE),
    )

    def get(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        hours = int(query_params["hours"]) if "hours" in query_params else None
        if audit_id:
            last_time = None
            first_time = None
            first_count = 0
            try:
                audit = AuditProcessor.objects.get(id=audit_id)
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                raise ValidationError("invalid audit_id: please check")
            history = AuditProcessorCache.objects.filter(audit=audit)
            if hours:
                history = history.filter(created__gt=timezone.now() - timedelta(hours=hours))
            try:
                first_time = history.order_by("id")[0].created
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
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
                if not first_count:
                    first_count = h.count
                last_time = h.created
                rate = h.count - previous if previous else None
                if position % every_other == 0:
                    res['results'].append({
                        'date': h.created.astimezone(pytz.timezone('America/Los_Angeles')).strftime("%m/%d %I:%M %p"),
                        'count': h.count,
                        'rate': rate,
                    })
                previous = h.count
                position += 1
            if audit.completed:
                first_time = audit.started
                last_time = audit.completed
                last_count = audit.cached_data.get('total', 0)
                first_count = 0
                try:
                    diff = (last_time - first_time)
                    minutes = (diff.total_seconds() / 60)
                    res['rate_average'] = (last_count - first_count) / minutes
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
            else:
                res['rate_average'] = audit.params.get('avg_rate_per_minute')
                if not res['rate_average']:
                    res['rate_average'] = 'N/A'
            try:
                res['elapsed_time'] = str(last_time - first_time).replace(",", "").split(".")[0]
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                res['elapsed_time'] = 'N/A'
            return Response(res)
