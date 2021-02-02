from datetime import timedelta
import json

from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.api.serializers.pacing_report_opportunities_serializer import \
    PacingReportOpportunitiesSerializer
from aw_reporting.api.views.pacing_report.constants import PACING_REPORT_OPPORTUNITIES_MAX_WATCH
from aw_reporting.reports.pacing_report import PacingReport
from cache.models import CacheItem
from dashboard.api.views.constants import DASHBOARD_PACING_ALERTS_CACHE_PREFIX
from dashboard.models import OpportunityWatch
from dashboard.utils import get_cache_key
from utils.datetime import now_in_default_tz
from userprofile.constants import StaticPermissions


class DashboardPacingAlertsAPIView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.PACING_REPORT),
    )
    CACHE_TTL = 1800
    MAX_SIZE = PACING_REPORT_OPPORTUNITIES_MAX_WATCH

    def get(self, request, *args, **kwargs):
        cache_key = self.get_cache_key(request.user.id)
        now = now_in_default_tz()
        try:
            cache = CacheItem.objects.get(key=cache_key)
            data = json.loads(cache.value)
            if cache.created_at < now - timedelta(seconds=self.CACHE_TTL):
                data = self._get_data(request.user)
                cache.value = json.dumps(data)
                cache.created_at = now
                cache.save()
        except CacheItem.DoesNotExist:
            data = self._get_data(request.user)
            CacheItem.objects.create(key=cache_key, value=json.dumps(data))
        return Response(data=data)

    def _get_data(self, user):
        pacing_filters = {"watch": True} if OpportunityWatch.objects.filter(user=user).exists() else \
            {"period": "this_month", "status": "active"}
        report = PacingReport().get_opportunities(pacing_filters, user, sort=["-has_alerts"], limit=self.MAX_SIZE)
        opportunities = PacingReportOpportunitiesSerializer(report, many=True).data
        return opportunities

    @staticmethod
    def get_cache_key(user_id):
        cache_key = get_cache_key(user_id, prefix=DASHBOARD_PACING_ALERTS_CACHE_PREFIX)
        return cache_key
