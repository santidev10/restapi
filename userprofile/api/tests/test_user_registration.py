import json

from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.test import APITestCase

from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from userprofile.constants import StaticPermissions
from userprofile.constants import UserAnnualAdSpend
from userprofile.constants import UserStatuses
from userprofile.constants import UserTypeRegular
from userprofile.models import UserProfile
from userprofile.models import PermissionItem
from utils.unittests.generic_test import generic_test
from utils.unittests.reverse import reverse


class UserRegistrationTestCase(APITestCase):
    registration_url = reverse(UserprofilePathName.CREATE_USER, [Namespace.USER_PROFILE])
    auth_url = reverse(UserprofilePathName.AUTH, [Namespace.USER_PROFILE])

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        PermissionItem.load_permissions()

    def _user_data(self, **kwargs):
        password = kwargs.get("password", "EmptyPassword1!")
        default_data = {
            "first_name": "Test",
            "last_name": "Test",
            "email": "test@example.com",
            "company": "test",
            "phone_number": "+380000000000",
            "password": password,
            "verify_password": password,
            "user_type": UserTypeRegular.AGENCY.value,
            "annual_ad_spend": UserAnnualAdSpend.SPEND_0_100K.value,
        }
        return {**default_data, **kwargs}

    def test_password_validation(self):
        """
        ensure password validators are working properly. See
        settings.PASSWORD_POLICY and serializer's passwords to
        change validation rules
        """
        bad_passwords = [
            "Short1!",
            "no_capitalization1!",
            "Nospecialchars1",
            "No_numbers!"
        ]
        for bad_password in bad_passwords:
            user_data = self._user_data(password=bad_password)
            response = None
            response = self.client.post(self.registration_url, data=user_data)
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            self.assertIsNotNone(response.data.get("verify_password", None))

    def test_registration_procedure_success(self):
        password = "Testing1!"
        email = "test@example.com"
        user_data = self._user_data(password=password)
        response = self.client.post(self.registration_url, data=user_data)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        user = get_user_model().objects.get(email=email)
        self.assertEqual(user.status, UserStatuses.PENDING.value)
        self.assertFalse(user.is_active)

    @generic_test([
        (user_type, (user_type.value,), dict())
        for user_type in UserTypeRegular
    ])
    def test_user_type_valid(self, user_type):
        self.assertTrue(UserTypeRegular.has_value(user_type))
        user_data = self._user_data(user_type=user_type)
        response = self.client.post(self.registration_url, data=user_data)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(UserProfile.objects.first().user_type, user_type)

    def test_user_type_not_allow_none(self):
        user_data = self._user_data(user_type=None)
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_user_type_is_required(self):
        user_data = self._user_data()
        user_data.pop("user_type")
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(UserProfile.objects.count(), 0)

    def test_user_type_invalid(self):
        user_type = "some value"
        self.assertFalse(UserTypeRegular.has_value(user_type))
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

    def test_annual_ad_spend_not_allow_empty(self):
        user_data = self._user_data()
        user_data["annual_ad_spend"] = ""
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_annual_ad_spend_not_allow_none(self):
        user_data = self._user_data(annual_ad_spend=None)
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_annual_ad_spend_invalid(self):
        annual_ad_spend = "some value"
        self.assertFalse(UserAnnualAdSpend.has_value(annual_ad_spend))
        user_data = self._user_data(annual_ad_spend=annual_ad_spend)
        response = self.client.post(self.registration_url, data=json.dumps(user_data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(UserProfile.objects.count(), 0)
