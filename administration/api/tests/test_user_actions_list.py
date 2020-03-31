from rest_framework.status import HTTP_403_FORBIDDEN, HTTP_200_OK

from administration.api.urls.names import AdministrationPathName
from utils.unittests.test_case import  ExtendedAPITestCase
from saas.urls.namespaces import Namespace
from django.urls import reverse

class UserActionsTestCase(ExtendedAPITestCase):
    url = reverse('{}:{}'.format(
        Namespace.ADMIN,
        AdministrationPathName.USER_ACTION_LIST
    ))

    def test_forbidden_access(self):
        """
        normal users should not have access
        """
        user = self.create_test_user()
        get_response = self.client.get(self.url)
        self.assertEqual(get_response.status_code, HTTP_403_FORBIDDEN)

    def test_admin_access(self):
        """
        user admin should have access
        """
        user = self.create_admin_user()
        get_response = self.client.get(self.url)
        self.assertEqual(get_response.status_code, HTTP_200_OK)
