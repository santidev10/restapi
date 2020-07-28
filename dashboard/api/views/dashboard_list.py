from django.contrib.auth import get_user_model
from rest_framework.views import APIView
from rest_framework.response import Response

from userprofile.permissions import PermissionGroupNames
from dashboard.constants import Dashboards
from utils.views import get_object


class DashboardListAPIView(APIView):
    DASHBOARD_PERMISSIONS = {
        PermissionGroupNames.RESEARCH: Dashboards.INDUSTRY_TOP_PERFORMERS,
        PermissionGroupNames.TOOLS: Dashboards.PACING_ALERTS,
        PermissionGroupNames.MEDIA_PLANNING_AUDIT: Dashboards.AUDIT_TOOL,
        PermissionGroupNames.MANAGED_SERVICE: Dashboards.PERFORMANCE,
    }

    def get(self, request, *args, **kwargs):
        user = get_object(get_user_model(), id=kwargs["pk"])
        dashboard = self.DASHBOARD_PERMISSIONS.values()
        return Response(data=dashboard)
