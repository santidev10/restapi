from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from administration.api.urls.names import AdministrationPathName
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class AdminDeleteUserTestCase(ExtendedAPITestCase):
    def test_reject_delete_admin_user(self):
        self.user = self.create_admin_user()
        url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(self.user.id,))
        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
