from unittest.mock import patch

from rest_framework.status import HTTP_200_OK

from dashboard.api.views.constants import Dashboards
from dashboard.api.urls.names import DashboardPathName
from dashboard.api.views import DashboardListAPIView
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase
from utils.unittests.reverse import reverse
from userprofile.models import WhiteLabel


class DashboardListTestCase(APITestCase):
    _url = reverse(DashboardPathName.DASHBOARD_LIST, [Namespace.DASHBOARD])

    def test_response_all(self):
        """ Test response if user has all permissions """
        dashboard_permissions = DashboardListAPIView.DASHBOARD_PERMISSIONS
        self.create_admin_user()
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data), set(dashboard_permissions.values()))

    def test_subdomain_feature_disabled(self):
        """
        Test that if a feature is disabled for a subdomain, it does not return as a valid dashboard element
            even if the user has permission for it
         """
        subdomain = WhiteLabel.objects.create(domain="test_subdomain_feature_disabled", config=dict(
            disable=["Analytics > Managed Service", "Research"]
        ))
        self.create_admin_user()
        with patch.object(WhiteLabel, "get", return_value=subdomain):
            response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data, [Dashboards.PACING_ALERTS, Dashboards.AUDIT_TOOL])
