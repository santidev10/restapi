import re

from django.core import mail
from django.urls import reverse
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST

from userprofile.models import UserProfile
from utils.unittests.test_case import ExtendedAPITestCase


class UserPasswordResetProcedureTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.password_reset_url = reverse(
            "userprofile_api_urls:password_reset")
        self.password_set_url = reverse("userprofile_api_urls:set_password")

    def test_obtain_reset_link_fail_user_does_not_exist(self):
        response = self.client.post(
            self.password_reset_url,
            data={"email": "notexistsemail@example.com"})
        self.assertEqual(HTTP_202_ACCEPTED, response.status_code)

    def test_obtain_reset_link_fail_user_is_admin(self):
        user = self.create_test_user()
        user.is_superuser = True
        user.save()
        response = self.client.post(
            self.password_reset_url,
            data={"email": user.email})
        self.assertEqual(HTTP_202_ACCEPTED, response.status_code)

    def test_success_obtain_reset_link(self):
        user = self.create_test_user(auth=False)
        response = self.client.post(
            self.password_reset_url,
            data={"email": user.email},
            headers={"content-type": "application/json"})
        self.assertEqual(HTTP_202_ACCEPTED, response.status_code)
        self.assertEqual(len(mail.outbox), 1)

    def test_set_password_fail_invalid_serialization(self):
        response = self.client.post(
            self.password_set_url,
            data={},
            headers={"content-type": "application/json"})
        self.assertEqual(HTTP_400_BAD_REQUEST, response.status_code)
        self.assertEqual(
            {"new_password", "email", "token"}, response.data.keys())

    def test_set_password_fail_invalid_email(self):
        response = self.client.post(
            self.password_set_url,
            data={"new_password": "Testing1!",
                  "email": "test@example.com",
                  "token": "123"},
            headers={"content-type": "application/json"})
        self.assertEqual(HTTP_400_BAD_REQUEST, response.status_code)

    def test_set_password_fail_invalid_token(self):
        user = self.create_test_user(auth=False)
        response = self.client.post(
            self.password_set_url,
            data={"new_password": "Testing1!",
                  "email": user.email,
                  "token": "123"},
            headers={"content-type": "application/json"})
        self.assertEqual(HTTP_400_BAD_REQUEST, response.status_code)

    def test_set_password_validation(self):
        user = self.create_test_user(auth=False)
        bad_passwords = [
            "Short1!",
            "no_capitalization1!",
            "Nospecialchars1",
            "No_numbers!"
        ]
        for bad_password in bad_passwords:
            response = self.client.post(
                self.password_set_url,
                data={
                    "new_password": bad_password,
                    "email": user.email,
                    "token": "123"
                },
                headers={"content-type": "application/json"}
            )
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            self.assertIsNotNone(response.data.get("new_password", None))

    def test_set_password_success(self):
        user = self.create_test_user(auth=False)
        new_passwords = ["Testing1!", "Testing2!", "Testing3!"]
        for i in range(3):
            self.client.post(
                self.password_reset_url,
                data={"email": user.email},
                headers={"content-type": "application/json"})
            token = re.search(
                r"token=[0-9A-Za-z]{1,13}-[0-9A-Za-z]{1,20}",
                mail.outbox[i].alternatives[0][0]).group(0).replace("token=", "")
            response = self.client.post(
                self.password_set_url,
                data={"new_password": new_passwords[i],
                      "email": user.email,
                      "token": token},
                headers={"content-type": "application/json"})
            self.assertEqual(HTTP_202_ACCEPTED, response.status_code)
            self.assertTrue(
                UserProfile.objects.get(
                    id=user.id).check_password(new_passwords[i]))
