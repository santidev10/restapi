from django.contrib.auth.models import Group
from rest_framework.status import HTTP_200_OK

from dashboard.api.urls.names import DashboardPathName
from dashboard.api.views import DashboardListAPIView
from saas.urls.namespaces import Namespace
from userprofile.permissions import Permissions
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase
from utils.unittests.reverse import reverse


class DashboardListTestCase(APITestCase):
    _url = reverse(DashboardPathName.DASHBOARD_LIST, [Namespace.DASHBOARD])

    @classmethod
    def setUpClass(cls):
        Permissions.sync_groups()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_response_all(self):
        """ Test response if user has all permissions """
        dashboard_permissions = DashboardListAPIView.DASHBOARD_PERMISSIONS
        user = self.create_admin_user()
        user.groups.add(*Group.objects.filter(name__in=dashboard_permissions.keys()))
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data), set(dashboard_permissions.values()))

    def test_none(self):
        """ Test response if user no permissions """
        self.create_admin_user()
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data), set())
