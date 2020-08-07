from datetime import timedelta
import json

from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.reports.pacing_report import PacingReport
from cache.models import CacheItem
from dashboard.api.serializers.dashboard_managed_service import DashboardManagedServiceOpportunityAdminSerialzer
from dashboard.api.serializers.dashboard_managed_service import DashboardManagedServiceOpportunitySerializer
from dashboard.api.views.constants import DASHBOARD_MANAGED_SERVICE_CACHE_PREFIX
from dashboard.utils import get_cache_key
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class DashboardManagedServiceAPIView(APIView):

    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_dashboard"),
            IsAdminUser
        ),
    )

    FIELDS = ['video_view_rate', 'ctr', 'video_quartile_100_rate', 'margin',
              'pacing', 'cpv']

    CACHE_TTL = 60 * 30

    def get(self, request, *args, **kwargs):
        cache_key = self.get_cache_key(request.user.id)
        try:
            cache = CacheItem.objects.get(key=cache_key)
            if cache.created_at < now_in_default_tz() - timedelta(seconds=self.CACHE_TTL):
                cache.value = self._get_data()
                cache.save()
            data = json.loads(cache.value)
        except CacheItem.DoesNotExist:
            data = self._get_data()
            CacheItem.objects.create(key=cache_key, value=json.dumps(data))
        return Response(data=data)

    def _get_data(self):
        aw_settings = self.request.user.get_aw_settings()
        visible_account_ids = aw_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS, [])
        opportunities = PacingReport().get_opportunities(get={}, user=self.request.user,
                                                         aw_cid=visible_account_ids,
                                                         managed_service_data=True)
        serializer_class = self.get_serializer_class()
        return {
            'averages': serializer_class(self.get_averages(opportunities)).data,
            'items': serializer_class(opportunities, many=True).data,
        }

    def get_serializer_class(self):
        """return different serializer depending on user perms"""
        if self.request.user.is_staff:
            return DashboardManagedServiceOpportunityAdminSerialzer
        return DashboardManagedServiceOpportunitySerializer

    def get_averages(self, opportunities):
        """
        compute mean averages for select Opportunity Fields
        """
        values = {}
        # TODO: add viewability/viewable_rate to default fields after VIQ2-428
        fields = self.FIELDS
        for opportunity in opportunities:
            for field_name in fields:
                field_values = values.get(field_name, [])
                field_values.append(opportunity.get(field_name, 0))
                values[field_name] = field_values
        averages = {}
        for field_name in fields:
            field_values = values.get(field_name, [])
            averages[field_name] = sum(field_values) / len(field_values) \
                if len(field_values) and sum(field_values) \
                else None
        return averages

    @staticmethod
    def get_cache_key(user_id):
        cache_key = get_cache_key(user_id, prefix=DASHBOARD_MANAGED_SERVICE_CACHE_PREFIX)
        return cache_key
