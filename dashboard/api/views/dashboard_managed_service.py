from rest_framework.views import APIView
from aw_reporting.reports.pacing_report import PacingReport
from rest_framework.response import Response
from userprofile.constants import UserSettingsKey
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from rest_framework.permissions import IsAdminUser
from dashboard.api.serializers.dashboard_managed_service import DashboardManagedServiceOpportunityAdminSerialzer
from dashboard.api.serializers.dashboard_managed_service import DashboardManagedServiceOpportunitySerializer


class DashboardManagedServiceAPIView(APIView):

    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_dashboard"),
            IsAdminUser
        ),
    )

    FIELDS = ['video_view_rate', 'ctr', 'video_quartile_100_rate', 'margin',
              'pacing', 'cpv']

    def get(self, request, *args, **kwargs):
        aw_settings = request.user.get_aw_settings()
        visible_account_ids = aw_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS, [])
        opportunities = PacingReport().get_opportunities(get={}, user=request.user,
                                                         aw_cid=visible_account_ids,
                                                         managed_service_data=True)
        serializer_class = self.get_serializer_class()
        data = {
            'averages': serializer_class(self.get_averages(opportunities)).data,
            'items': serializer_class(opportunities, many=True).data,
        }
        return Response(data=data)

    def get_serializer_class(self):
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
