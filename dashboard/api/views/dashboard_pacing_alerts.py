from datetime import timedelta
import json

from rest_framework.views import APIView
from rest_framework.response import Response

from aw_reporting.api.serializers.pacing_report_opportunities_serializer import \
    PacingReportOpportunitiesSerializer
from aw_reporting.reports.pacing_report import PacingReport
from cache.models import CacheItem
from dashboard.utils import get_cache_key
from utils.datetime import now_in_default_tz


class DashboardPacingAlertsAPIView(APIView):
    CACHE_PREFIX = "dashboard_pacing_alerts_"
    CACHE_TTL = 3600

    def get(self, request, *args, **kwargs):
        params = str(request.query_params) + str(kwargs)
        cache_key = get_cache_key(params, prefix=self.CACHE_PREFIX)
        try:
            cache = CacheItem.objects.get(key=cache_key).value
            if cache.updated_at < now_in_default_tz() - timedelta(seconds=self.CACHE_TTL):
                cache.value = self._get_data(request.user)
                cache.save()
            data = cache.value
        except CacheItem.DoesNotExist:
            data = self._get_data(request.user)
            CacheItem.objects.create(key=cache_key, value=json.dumps(data))
        return Response(data=data)

    def _get_data(self, user):
        report = PacingReport()
        opportunities = report.get_opportunities({}, number=[op.number for op in user.opportunities.all()])
        data = PacingReportOpportunitiesSerializer(opportunities, many=True)
        return data
