from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_404_NOT_FOUND, \
    HTTP_403_FORBIDDEN, HTTP_400_BAD_REQUEST, HTTP_202_ACCEPTED

from saas.utils_tests import ExtendedAPITestCase
from userprofile.models import UserProfile


class UserPasswordResetProcedureTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.password_reset_url = reverse(
            "userprofile_api_urls:password_reset")
        self.password_set_url = reverse("userprofile_api_urls:set_password")

    def test_obtain_reset_link_fail_user_does_not_exist(self):
        response = self.client.post(
            self.password_reset_url,
            data={"email": "notexistsemail@example.com"})
        self.assertEqual(HTTP_404_NOT_FOUND, response.status_code)

    def test_obtain_reset_link_fail_user_is_admin(self):
        user = self.create_test_user()
        user.is_superuser = True
        user.save()
        response = self.client.post(
            self.password_reset_url,
            data={"email": user.email})
        self.assertEqual(HTTP_403_FORBIDDEN, response.status_code)

    @patch("userprofile.api.views.send_html_email")
    def test_success_obtain_reset_link(self, send_html_email_mock):
        user = self.create_test_user(False)
        response = self.client.post(
            self.password_reset_url,
            data={"email": user.email},
            headers={"content-type": "application/json"})
        self.assertEqual(HTTP_200_OK, response.status_code)
        self.assertEqual({"reset_url", "token", "email"}, response.data.keys())
        message = "Click the link below to reset your password.\n" \
                  "{}\n\n" \
                  "Please do not respond to this email.\n\n" \
                  "Kind regards, Channel Factory Team".format(
                        response.data.get("reset_url"))
        send_html_email_mock.assert_called_with(
            "SaaS > Password reset notification",
            user.email,
            "Dear {} \n".format(user.get_full_name()),
            message,
            "testserver")

    def test_set_password_fail_invalid_serialization(self):
        response = self.client.post(
            self.password_set_url,
            data={},
            headers={"content-type": "application/json"})
        self.assertEqual(HTTP_400_BAD_REQUEST, response.status_code)
        self.assertEqual(
            {"new_password", "email", "token"}, response.data.keys())

    def test_set_password_fail_invalid_user(self):
        response = self.client.post(
            self.password_set_url,
            data={"new_password": "test",
                  "email": "test@example.com",
                  "token": "123"},
            headers={"content-type": "application/json"})
        self.assertEqual(HTTP_404_NOT_FOUND, response.status_code)

    def test_set_password_fail_invalid_token(self):
        user = self.create_test_user(False)
        response = self.client.post(
            self.password_set_url,
            data={"new_password": "test",
                  "email": user.email,
                  "token": "123"},
            headers={"content-type": "application/json"})
        self.assertEqual(HTTP_400_BAD_REQUEST, response.status_code)

    def test_set_password_success(self):
        def execute_reset_procedure(new_password):
            user = self.create_test_user(False)
            response = self.client.post(
                self.password_reset_url,
                data={"email": user.email},
                headers={"content-type": "application/json"})
            token = response.data.get("token")
            response = self.client.post(
                self.password_set_url,
                data={"new_password": new_password,
                      "email": user.email,
                      "token": token},
                headers={"content-type": "application/json"})
            self.assertEqual(HTTP_202_ACCEPTED, response.status_code)
            self.assertTrue(
                UserProfile.objects.get(
                    id=user.id).check_password(new_password))
        for i in range(3):
            new_passwords = ["admin", "admin2", "admin3"]
            execute_reset_procedure(new_passwords[i])
