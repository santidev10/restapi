from utils.unittests.test_case import ExtendedAPITestCase
from django.urls import reverse
from saas.urls.namespaces import Namespace
from aw_reporting.api.urls.names import Name
from urllib.parse import urlencode
import json
from rest_framework.status import (
    HTTP_403_FORBIDDEN,
    HTTP_202_ACCEPTED,
    HTTP_200_OK
)

class VisibleAccountsTestCase(ExtendedAPITestCase):
    url = reverse('{}:{}'.format(
        Namespace.AW_REPORTING,
        Name.Admin.USER_AW_SETTINGS
    ))

    def test_forbidden_access(self):
        """
        regular users and unauthenticated users should not be able
        to access this endpoint
        """
        user = self.create_test_user()
        query_params = {'user_id': user.id}
        url = '{}?{}'.format(self.url, urlencode(query_params))
        get_response = self.client.get(url)
        self.assertEqual(get_response.status_code, HTTP_403_FORBIDDEN)

        put_response = self.make_put_request(url)
        self.assertEqual(put_response.status_code, HTTP_403_FORBIDDEN)


    def test_admin_access(self):
        """
        only admin users should be able to access this endpoint
        """
        user = self.create_admin_user()
        query_params = {'user_id': user.id}
        url = '{}?{}'.format(self.url, urlencode(query_params))
        get_response = self.client.get(url)
        self.assertEqual(get_response.status_code, HTTP_200_OK)

        put_response = self.make_put_request(url)
        self.assertEqual(put_response.status_code, HTTP_202_ACCEPTED)
        self.assertEqual(put_response.data['show_conversions'], True)

    def get_put_data(self):
        return json.dumps({
            'show_conversions': True,
        })

    def make_put_request(self, url, data=None):
        if not data:
            data = self.get_put_data()

        return self.client.put(
            url,
            data=data,
            content_type='application/json'
        )