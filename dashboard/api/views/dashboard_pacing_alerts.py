from datetime import timedelta
import json

from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.api.serializers.pacing_report_opportunities_serializer import \
    PacingReportOpportunitiesSerializer
from aw_reporting.reports.pacing_report import PacingReport
from cache.models import CacheItem
from dashboard.utils import get_cache_key
from utils.datetime import now_in_default_tz


class DashboardPacingAlertsAPIView(APIView):
    CACHE_PREFIX = "dashboard_pacing_alerts_"
    CACHE_TTL = 1800

    def get(self, request, *args, **kwargs):
        cache_key = get_cache_key(request.user.id, prefix=self.CACHE_PREFIX)
        try:
            cache = CacheItem.objects.get(key=cache_key)
            if cache.updated_at < now_in_default_tz() - timedelta(seconds=self.CACHE_TTL):
                cache.value = self._get_data(request.user)
                cache.save()
            data = cache.value
        except CacheItem.DoesNotExist:
            data = self._get_data(request.user)
            CacheItem.objects.create(key=cache_key, value=json.dumps(data))
        return Response(data=data)

    def _get_data(self, user):
        report = PacingReport().get_opportunities({"watch": True}, user)
        opportunities = PacingReportOpportunitiesSerializer(report, many=True).data
        # Sort by name then by alerts length
        data = sorted(opportunities, key=lambda op: (op["name"], len(op.get("alerts", []))))
        return data
