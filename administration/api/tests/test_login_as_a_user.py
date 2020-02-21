from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN, \
    HTTP_404_NOT_FOUND

from userprofile.models import UserDeviceToken
from utils.utittests.test_case import ExtendedAPITestCase


class LoginAsAUserAPITestCase(ExtendedAPITestCase):

    def test_fail_permission(self):
        self.user = self.create_test_user()
        url = reverse("admin_api_urls:user_auth_admin",
                      args=(self.user.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_fail_as_admin(self):
        self.user = self.create_admin_user()
        url = reverse("admin_api_urls:user_auth_admin",
                      args=(self.user.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_fail_404(self):
        self.user = self.create_test_user()
        self.user.is_staff = True
        self.user.save()

        url = reverse("admin_api_urls:user_auth_admin",
                      args=(self.user.id + 1,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success(self):
        self.user = self.create_test_user()
        self.user.is_staff = True
        self.user.save()
        test_user = get_user_model().objects.create(
            email="mr_bond_james_bond@mail.kz"
        )
        url = reverse("admin_api_urls:user_auth_admin",
                      args=(test_user.id,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_new_auth_token(self):
        self.user = self.create_test_user()
        self.user.is_staff = True
        self.user.save()
        token = self.user.tokens.first()
        test_user = get_user_model().objects.create(
            email="mr_bond_james_bond@mail.kz"
        )
        url = reverse("admin_api_urls:user_auth_admin",
                      args=(test_user.id,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        logged_in_token = UserDeviceToken.objects.get(key=response.data["token"])
        self.assertNotEqual(token.key, logged_in_token.key)
        self.assertNotEqual(logged_in_token.user_id, self.user.id)
        self.assertEqual(logged_in_token.user_id, test_user.id)
