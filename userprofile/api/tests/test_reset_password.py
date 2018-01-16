import json
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_404_NOT_FOUND, HTTP_403_FORBIDDEN

from saas.utils_tests import ExtendedAPITestCase


class UserPasswordResetAPITestCase(ExtendedAPITestCase):
    def make_request(self, email):
        url = reverse("userprofile_api_urls:password_reset")
        return self.client.post(url, json.dumps(dict(email=email)), content_type='application/json')

    def test_success(self):
        user = self.create_test_user(False)
        response = self.make_request(user.email)
        self.assertEqual(response.status_code, HTTP_200_OK)

    @patch('userprofile.api.views.send_html_email')
    def test_success_email_send_with_html_template(self, send_html_email_mock):
        user = self.create_test_user(False)
        self.make_request(user.email)
        self.assertTrue(send_html_email_mock.called)

    def test_failure_when_user_doesnt_exist(self):
        not_existing_email = "some_mail@mail.com"
        response = self.make_request(not_existing_email)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
