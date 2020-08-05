from datetime import timedelta
import json

from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.api.serializers.pacing_report_opportunities_serializer import \
    PacingReportOpportunitiesSerializer
from aw_reporting.reports.pacing_report import PacingReport
from cache.models import CacheItem
from dashboard.api.views.constants import DASHBOARD_PACING_ALERTS_CACHE_PREFIX
from dashboard.utils import get_cache_key
from utils.datetime import now_in_default_tz
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class DashboardPacingAlertsAPIView(APIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_pricing_tool"),
            user_has_permission("userprofile.view_chf_trends"),
        ),
    )
    CACHE_TTL = 1800

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
        report = PacingReport().get_opportunities({"watch": True}, user)
        opportunities = PacingReportOpportunitiesSerializer(report, many=True).data
        # Sort by name then by alerts length
        data = sorted(opportunities, key=lambda op: (op["name"], len(op.get("alerts", []))))
        return data

    @staticmethod
    def get_cache_key(user_id):
        cache_key = get_cache_key(user_id, prefix=DASHBOARD_PACING_ALERTS_CACHE_PREFIX)
        return cache_key
