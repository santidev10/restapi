from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_reporting.api.tests.base import AwReportingAPITestCase
import json


class AuthAPITestCase(AwReportingAPITestCase):

    def test_success(self):
        user = self.create_test_user()
        url = reverse("userprofile_api_urls:user_auth")
        response = self.client.post(
            url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data),
            {
                'id', 'profile_image_url', 'company', 'phone_number', 'is_staff', 'last_name', 'has_aw_accounts',
                'date_joined', 'last_login', 'plan', 'email', 'first_name', 'token', 'can_access_media_buying',
                'has_paid_subscription_error',
            }
        )

    def test_success_has_connected_accounts(self):
        user = self.create_test_user()
        self.create_account(user)
        url = reverse("userprofile_api_urls:user_auth")
        response = self.client.post(
            url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data['has_aw_accounts'], True)

    def test_success_has_no_connected_accounts(self):
        user = self.create_test_user()
        url = reverse("userprofile_api_urls:user_auth")
        response = self.client.post(
            url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data['has_aw_accounts'], False)


