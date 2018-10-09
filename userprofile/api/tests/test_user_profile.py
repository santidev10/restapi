import json
from unittest import skip

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, \
    HTTP_401_UNAUTHORIZED

from aw_reporting.models import AWConnectionToUserRelation, AWConnection
from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from userprofile.constants import UserType, UserAnnualAdSpend
from userprofile.models import UserProfile
from utils.utils_tests import ExtendedAPITestCase, generic_test


class UserProfileTestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.USER_PROFILE + ":" + UserprofilePathName.USER_PROFILE)

    def _update(self, data):
        return self.client.put(self._url, json.dumps(data),
                               content_type="application/json")

    def test_require_auth(self):
        response = self._update(dict())

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_success(self):
        self.create_test_user()
        response = self._update(dict())
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                "access",
                "annual_ad_spend",
                "aw_settings",
                "can_access_media_buying",
                "company",
                "date_joined",
                "email",
                "first_name",
                "google_account_id",
                "has_aw_accounts",
                "has_disapproved_ad",
                "historical_aw_account",
                "id",
                "is_staff",
                "is_subscribed",
                "last_login",
                "last_name",
                "logo_url",
                "phone_number",
                "profile_image_url",
                "token",
            }
        )

    def test_set_default_aw_account_error_on_wrong_connection(self):
        user = self.create_test_user()
        any_user = UserProfile.objects.create()
        user_connection = AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="test_email"),
            user=any_user
        )
        self.assertIsNone(user.historical_aw_account)

        data = dict(historical_aw_account=user_connection.id)
        response = self._update(data)

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        user.refresh_from_db()
        self.assertIsNone(user.historical_aw_account)

    def test_set_default_aw_account_success_set(self):
        user = self.create_test_user()
        user_connection = AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="test_email"),
            user=user
        )
        self.assertIsNone(user.historical_aw_account)

        data = dict(historical_aw_account=user_connection.id)
        response = self._update(data)

        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertIsNotNone(user.historical_aw_account)

    def test_unset_default_aw_account(self):
        user = self.create_test_user()
        user_connection = AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="test_email"),
            user=user
        )
        user.historical_aw_account = user_connection
        user.save()
        self.assertIsNotNone(user.historical_aw_account)

        data = dict(historical_aw_account=None)
        response = self._update(data)

        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertIsNone(user.historical_aw_account)

    @skip("User Type disabled")
    @generic_test([
        (user_type, (user_type.value,), dict())
        for user_type in UserType
    ])
    def test_user_type_valid(self, user_type):
        user = self.create_test_user()
        self.assertIsNone(user.user_type)
        self.assertNotEqual(user.user_type, user_type)
        response = self._update(dict(user_type=user_type))
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertEqual(user.user_type, user_type)

    @skip("User Type disabled")
    def test_user_type_allow_unset(self):
        user = self.create_test_user()
        user.user_type = UserType.AGENCY.value
        user.save()
        response = self._update(dict(user_type=None))
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertIsNone(user.user_type)

    def test_user_type_not_required(self):
        self.create_test_user()
        response = self._update(dict())
        self.assertEqual(response.status_code, HTTP_200_OK)

    @skip("User Type disabled")
    def test_user_type_invalid(self):
        pre_user_type = UserType.AGENCY.value
        test_value = "invalid_value"
        self.assertFalse(UserType.has_value(test_value))
        user = self.create_test_user()
        user.user_type = pre_user_type
        user.save()
        response = self._update(dict(user_type=test_value))
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        user.refresh_from_db()
        self.assertEqual(user.user_type, pre_user_type)

    @generic_test([
        (annual_ad_spend, (annual_ad_spend.value,), dict())
        for annual_ad_spend in UserAnnualAdSpend
    ])
    def test_annual_ad_spend_valid(self, annual_ad_spend):
        user = self.create_test_user()
        self.assertIsNone(user.annual_ad_spend)
        self.assertNotEqual(user.annual_ad_spend, annual_ad_spend)
        response = self._update(dict(annual_ad_spend=annual_ad_spend))
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertEqual(user.annual_ad_spend, annual_ad_spend)

    def test_annual_ad_spend_allow_unset(self):
        user = self.create_test_user()
        user.annual_ad_spend = UserAnnualAdSpend.SPEND_100K_250K.value
        user.save()
        response = self._update(dict(annual_ad_spend=None))
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertIsNone(user.annual_ad_spend)

    def test_annual_ad_spend_not_required(self):
        self.create_test_user()
        response = self._update(dict())
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_annual_ad_spend_invalid(self):
        pre_user_annual_ad_spend = UserAnnualAdSpend.SPEND_100K_250K.value
        test_value = "invalid_value"
        self.assertFalse(UserAnnualAdSpend.has_value(test_value))
        user = self.create_test_user()
        user.annual_ad_spend = pre_user_annual_ad_spend
        user.save()
        response = self._update(dict(annual_ad_spend=test_value))
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        user.refresh_from_db()
        self.assertEqual(user.annual_ad_spend, pre_user_annual_ad_spend)

    @generic_test([
        (is_subscribed, (is_subscribed,), dict())
        for is_subscribed in (True, False)
    ])
    def test_is_subscribed_valid(self, is_subscribed):
        user = self.create_test_user()
        UserProfile.objects.filter(id=user.id).update(is_subscribed=not is_subscribed)
        user.refresh_from_db()
        self.assertNotEqual(user.is_subscribed, is_subscribed)

        response = self._update(dict(is_subscribed=is_subscribed))
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertEqual(user.is_subscribed, is_subscribed)

    def test_is_subscribed_optional(self):
        self.create_test_user()
        response = self._update(dict())
        self.assertEqual(response.status_code, HTTP_200_OK)

    @generic_test([
        ("Null", (None,), dict()),
        ("Number", (123,), dict()),
        ("String", ("qwer",), dict()),
    ])
    def test_is_subscribed_invalid(self, is_subscribed):
        self.create_test_user()
        response = self._update(dict(is_subscribed=is_subscribed))
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
