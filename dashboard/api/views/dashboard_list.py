from rest_framework.views import APIView
from rest_framework.response import Response

from dashboard.api.views.constants import Dashboards
from userprofile.constants import StaticPermissions


class DashboardListAPIView(APIView):
    DASHBOARD_PERMISSIONS = {
        StaticPermissions.RESEARCH: Dashboards.INDUSTRY_TOP_PERFORMERS,
        StaticPermissions.PACING_REPORT: Dashboards.PACING_ALERTS,
        StaticPermissions.AUDIT_QUEUE: Dashboards.AUDIT_TOOL,
        StaticPermissions.MANAGED_SERVICE: Dashboards.PERFORMANCE,
    }

    def get(self, request, *args, **kwargs):
        dashboard = []
        for dash_perm in self.DASHBOARD_PERMISSIONS.keys():
            if request.user.has_permission(dash_perm):
                dashboard.append(self.DASHBOARD_PERMISSIONS[dash_perm])
        return Response(data=dashboard)
