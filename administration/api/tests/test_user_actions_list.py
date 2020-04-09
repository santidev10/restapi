import json
from rest_framework.status import (
    HTTP_403_FORBIDDEN,
    HTTP_200_OK,
    HTTP_201_CREATED
)
from administration.api.urls.names import AdministrationPathName
from utils.unittests.test_case import  ExtendedAPITestCase
from saas.urls.namespaces import Namespace
from django.urls import reverse

class UserActionsTestCase(ExtendedAPITestCase):
    url = reverse('{}:{}'.format(
        Namespace.ADMIN,
        AdministrationPathName.USER_ACTION_LIST
    ))

    def test_is_authenticated_access(self):
        """
        normal users should not have access
        """
        user = self.create_test_user()
        get_response = self.client.get(self.url)
        self.assertEqual(get_response.status_code, HTTP_403_FORBIDDEN)

        post_response = self.client.post(
            self.url,
            data=self.get_post_data(),
            content_type='application/json'
        )
        self.assertEqual(post_response.status_code, HTTP_201_CREATED)

    def test_is_admin_access(self):
        """
        user admin should have access
        """
        user = self.create_admin_user()
        get_response = self.client.get(self.url)
        self.assertEqual(get_response.status_code, HTTP_200_OK)

        post_response = self.client.post(
            self.url,
            data=self.get_post_data(),
            content_type='application/json'
        )
        self.assertEqual(post_response.status_code, HTTP_201_CREATED)

    def get_post_data(self):
        return json.dumps({
                'url': 'https://saas.channelfactory.com/',
                'slug': 'home page'
            })
