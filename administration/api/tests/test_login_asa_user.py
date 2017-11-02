from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN, \
    HTTP_404_NOT_FOUND
from saas.utils_tests import ExtendedAPITestCase
from django.contrib.auth import get_user_model


class LoginAsAUserAPITestCase(ExtendedAPITestCase):

    def test_fail_permission(self):
        self.user = self.create_test_user()
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
        self.assertEqual(
            set(response.data.keys()),
            {
                'id',
                'company',
                'first_name',
                'phone_number',
                'email',
                'last_login',
                'token',
                'date_joined',
                'last_name',
                'is_staff',
                'has_aw_accounts',
                'profile_image_url',
                'plan',
            }
        )

