import json

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_403_FORBIDDEN

from administration.api.urls.names import AdministrationPathName
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase


class UserActionsTestCase(ExtendedAPITestCase):
    url = reverse("{}:{}".format(
        Namespace.ADMIN,
        AdministrationPathName.USER_ACTION_LIST
    ))

    def test_is_authenticated_access(self):
        """
        normal users should not have access
        """
        self.create_test_user()
        get_response = self.client.get(self.url)
        self.assertEqual(get_response.status_code, HTTP_403_FORBIDDEN)

        post_response = self.client.post(
            self.url,
            data=self.get_post_data(),
            content_type="application/json"
        )
        self.assertEqual(post_response.status_code, HTTP_201_CREATED)

    def test_is_admin_access(self):
        """
        user admin should have access
        """
        self.create_admin_user()
        get_response = self.client.get(self.url)
        self.assertEqual(get_response.status_code, HTTP_200_OK)

        post_response = self.client.post(
            self.url,
            data=self.get_post_data(),
            content_type="application/json"
        )
        self.assertEqual(post_response.status_code, HTTP_201_CREATED)

    def get_post_data(self):
        return json.dumps({
            "url": "https://saas.channelfactory.com/",
            "slug": "home page"
        })
