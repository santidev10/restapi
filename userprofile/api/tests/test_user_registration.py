import json

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.test import APITestCase

from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from userprofile.constants import UserType, UserAnnualAdSpend
from userprofile.models import UserProfile
from utils.utils_tests import generic_test
from utils.utils_tests import reverse


class UserRegistrationTestCase(APITestCase):
    registration_url = reverse(UserprofilePathName.CREATE_USER, [Namespace.USER_PROFILE])
    auth_url = reverse(UserprofilePathName.AUTH, [Namespace.USER_PROFILE])

    def _user_data(self, **kwargs):
        password = kwargs.get("password", "empty")
        default_data = {
            "first_name": "Test",
            "last_name": "Test",
            "email": "test@example.com",
            "company": "test",
            "phone_number": "+380000000000",
            "password": password,
            "verify_password": password,
            "user_type": UserType.AGENCY.value,
        }
        return {**default_data, **kwargs}

    def test_registration_procedure_success(self):
        password = "test"
        email = "test@example.com"
        user_data = self._user_data(password=password)
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

    @generic_test([
        (user_type, (user_type.value,), dict())
        for user_type in UserType
    ])
    def test_user_type_valid(self, user_type):
        self.assertTrue(UserType.has_value(user_type))
        user_data = self._user_data(user_type=user_type)
        response = self.client.post(self.registration_url, data=user_data)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(UserProfile.objects.first().user_type, user_type)

    def test_user_type_allow_none(self):
        user_data = self._user_data(user_type=None)
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertIsNone(UserProfile.objects.first().user_type)

    def test_user_type_is_required(self):
        user_data = self._user_data()
        user_data.pop("user_type")
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(UserProfile.objects.count(), 0)

    def test_user_type_invalid(self):
        user_type = "some value"
        self.assertFalse(UserType.has_value(user_type))
        user_data = self._user_data(user_type=user_type)
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(UserProfile.objects.count(), 0)

    @generic_test([
        (annual_ad_spend, (annual_ad_spend.value,), dict())
        for annual_ad_spend in UserAnnualAdSpend
    ])
    def test_annual_ad_spend_valid(self, annual_ad_spend):
        self.assertTrue(UserAnnualAdSpend.has_value(annual_ad_spend))
        user_data = self._user_data(annual_ad_spend=annual_ad_spend)
        response = self.client.post(self.registration_url, data=user_data)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(UserProfile.objects.first().annual_ad_spend, annual_ad_spend)

    def test_annual_ad_spend_allow_empty(self):
        user_data = self._user_data()
        self.assertNotIn("annual_ad_spend", user_data)
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertIsNone(UserProfile.objects.first().annual_ad_spend)

    def test_annual_ad_spend_allow_none(self):
        user_data = self._user_data(annual_ad_spend=None)
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertIsNone(UserProfile.objects.first().annual_ad_spend)

    def test_annual_ad_spend_invalid(self):
        annual_ad_spend = "some value"
        self.assertFalse(UserAnnualAdSpend.has_value(annual_ad_spend))
        user_data = self._user_data(annual_ad_spend=annual_ad_spend)
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(UserProfile.objects.count(), 0)

    @generic_test([
        (is_subscribed, (is_subscribed,), dict())
        for is_subscribed in (True, False)
    ])
    def test_is_subscribed_valid(self, is_subscribed):
        user_data = self._user_data(is_subscribed=is_subscribed)
        response = self.client.post(self.registration_url, data=user_data)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(UserProfile.objects.first().is_subscribed, is_subscribed)

    def test_is_subscribed_optional(self):
        user_data = self._user_data()
        response = self.client.post(self.registration_url, data=user_data)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertFalse(UserProfile.objects.first().is_subscribed)

    @generic_test([
        ("Null", (None,), dict()),
        ("Number", (123,), dict()),
        ("String", ("qwer",), dict()),
    ])
    def test_is_subscribed_invalid(self, is_subscribed):
        user_data = self._user_data(is_subscribed=is_subscribed)
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(UserProfile.objects.count(), 0)
