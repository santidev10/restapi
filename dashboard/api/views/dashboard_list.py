from rest_framework.views import APIView
from rest_framework.response import Response

from dashboard.api.views.constants import Dashboards
from userprofile.permissions import PermissionGroupNames


class DashboardListAPIView(APIView):
    DASHBOARD_PERMISSIONS = {
        PermissionGroupNames.RESEARCH: Dashboards.INDUSTRY_TOP_PERFORMERS,
        PermissionGroupNames.TOOLS: Dashboards.PACING_ALERTS,
        PermissionGroupNames.MEDIA_PLANNING_AUDIT: Dashboards.AUDIT_TOOL,
        PermissionGroupNames.MANAGED_SERVICE: Dashboards.PERFORMANCE,
    }

    def get(self, request, *args, **kwargs):
        dashboard = []
        user_permissions = set(request.user.groups.all().values_list("name", flat=True))
        for dash_perm in self.DASHBOARD_PERMISSIONS.keys():
            if dash_perm in user_permissions:
                dashboard.append(self.DASHBOARD_PERMISSIONS[dash_perm])
        return Response(data=dashboard)
