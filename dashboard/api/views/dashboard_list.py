from rest_framework.views import APIView
from rest_framework.response import Response

from dashboard.api.views.constants import Dashboards
from userprofile.constants import StaticPermissions
from userprofile.models import WhiteLabel


class DashboardListAPIView(APIView):
    DASHBOARD_PERMISSIONS = {
        StaticPermissions.RESEARCH: Dashboards.INDUSTRY_TOP_PERFORMERS,
        StaticPermissions.PACING_REPORT: Dashboards.PACING_ALERTS,
        StaticPermissions.AUDIT_QUEUE: Dashboards.AUDIT_TOOL,
        StaticPermissions.MANAGED_SERVICE: Dashboards.PERFORMANCE,
    }
    # Mapping of subdomain feature names
    DOMAIN_FEATURE_EXCLUSIONS = {
        "Analytics > Managed Service": StaticPermissions.MANAGED_SERVICE,
        "Research": StaticPermissions.RESEARCH,
        "Tools > Audit Queue": StaticPermissions.AUDIT_QUEUE,
        "Tools > Pacing Report": StaticPermissions.PACING_REPORT,
    }

    def get(self, request, *args, **kwargs):
        dashboard = []

        # Check for features that are disabled for the current subdomain
        sub_domain = WhiteLabel.get(domain=WhiteLabel.extract_sub_domain(request.get_host() or ""))
        disabled_features = set(
            self.DOMAIN_FEATURE_EXCLUSIONS[feature_name]
            for feature_name in sub_domain.config.get("disable", []) if feature_name in self.DOMAIN_FEATURE_EXCLUSIONS
        )

        for dash_perm in self.DASHBOARD_PERMISSIONS.keys():
            if request.user.has_permission(dash_perm) and dash_perm not in disabled_features:
                dashboard.append(self.DASHBOARD_PERMISSIONS[dash_perm])
        return Response(data=dashboard)
