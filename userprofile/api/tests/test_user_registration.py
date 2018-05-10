from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_201_CREATED, HTTP_200_OK
from rest_framework.test import APITestCase


class UserRegistrationTestCase(APITestCase):

    def setUp(self):
        self.registration_url = reverse("userprofile_api_urls:user_create")
        self.auth_url = reverse("userprofile_api_urls:user_auth")

    def test_registration_procedure_success(self):
        password = "test"
        email = "test@example.com"
        user_data = {
            "first_name": "Test",
            "last_name": "Test",
            "email": "test@example.com",
            "company": "test",
            "phone_number": "+380000000000",
            "password": password,
            "verify_password": password
        }
        response = self.client.post(self.registration_url, data=user_data)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.client.credentials(
            HTTP_AUTHORIZATION='Token {}'.format(response.data["token"]))
        response = self.client.delete(self.auth_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.client.credentials()
        response = self.client.post(
            self.auth_url, data={"username": email, "password": password})
        self.assertEqual(response.status_code, HTTP_200_OK)
