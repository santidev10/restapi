import json
import os

from django.contrib.auth import get_user_model
from django.db import IntegrityError
from django.test import override_settings
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED

from aw_reporting.api.tests.base import AWAccountPermission
from aw_reporting.api.tests.base import AWConnection
from aw_reporting.api.tests.base import AWConnectionToUserRelation
from aw_reporting.api.tests.base import Account
from aw_reporting.api.tests.base import Ad
from aw_reporting.api.tests.base import AdGroup
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.tests.base import Campaign
from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from userprofile.models import UserDeviceToken
from utils.unittests.reverse import reverse

CUSTOM_AUTH_FLAGS = {
    "test.user@testuser.com": {
        "hide_brand_name": True,
        "logo_url": "https://s3.amazonaws.com/viewiq-rc/logos/simon.png",
    },
    "test.apex_user@testuser.com": {
        "hide_brand_name": True,
        "is_apex": True,
        "logo_url": "https://s3.amazonaws.com/viewiq-rc/logos/apex.png",
    }
}


class AuthAPITestCase(AwReportingAPITestCase):
    _url = reverse(UserprofilePathName.AUTH, [Namespace.USER_PROFILE])

    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    def test_success(self):
        user = self.create_test_user()
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.tokens.first().key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data),
            {
                "aw_settings",
                "can_access_media_buying",
                "company",
                "date_joined",
                "device_id",
                "domain",
                "email",
                "first_name",
                "google_account_id",
                "has_aw_accounts",
                "has_disapproved_ad",
                "id", "profile_image_url",
                "is_staff",
                "last_login",
                "last_name",
                "logo_url",
                "perms",
                "phone_number",
                "phone_number_verified",
                "token",
                "is_active",
                "has_accepted_GDPR",
                "user_type",
            }
        )

    def test_success_has_connected_accounts(self):
        user = self.create_test_user()
        self.create_account(user)
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.tokens.first().key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["has_aw_accounts"], True)

    def test_success_has_no_connected_accounts(self):
        user = self.create_test_user()
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.tokens.first().key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["has_aw_accounts"], False)

    def test_success_has_no_disapproved_ad(self):
        user = self.create_test_user()
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.tokens.first().key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["has_disapproved_ad"], False)

    def test_success_has_disapproved_ad(self):
        user = self.create_test_user()
        account = Account.objects.create(id="1", name="",
                                         can_manage_clients=True)
        connection = AWConnection.objects.create()
        AWAccountPermission.objects.create(aw_connection=connection,
                                           account=account, can_read=True)
        AWConnectionToUserRelation.objects.create(
            connection=connection,
            user=user
        )
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        Ad.objects.create(ad_group=ad_group, is_disapproved=True)
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.tokens.first().key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["has_disapproved_ad"], True)

    def test_case_insensitive(self):
        email = "MixedCase@Email.com"
        password = "test_password"
        user = get_user_model().objects.create(
            email=email
        )
        user.set_password(password)
        user.save()

        payload_exact = dict(username=email, password=password)
        payload_upper = dict(username=email.upper(), password=password)
        payload_lower = dict(username=email.lower(), password=password)

        for payload in (payload_exact, payload_upper, payload_lower):
            response = self.client.post(self._url,
                                        json.dumps(payload),
                                        content_type="application/json")

            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_user_email_should_be_stored_in_lowercase(self):
        test_email = "Test@email.com"
        test_email_lower = test_email.lower()
        self.assertNotEqual(test_email, test_email.lower())

        user = get_user_model().objects.create(email=test_email)

        stored_in_lowercase = get_user_model().objects \
            .filter(id=user.id, email=test_email_lower) \
            .exists()
        self.assertTrue(stored_in_lowercase)

        user.refresh_from_db()
        self.assertEqual(user.email, test_email_lower)

    def test_user_unique_by_email_case_insensitive(self):
        test_email = "Test@email.com"

        def create_user(email):
            return get_user_model().objects.create(email=email)

        create_user(test_email)
        try:
            create_user(test_email.upper())
        except IntegrityError:
            pass
        else:
            self.fail()

    @override_settings(CUSTOM_AUTH_FLAGS=CUSTOM_AUTH_FLAGS)
    def test_success_apex_user_auth(self):
        user = self.create_test_user()
        user.email = "test.apex_user@testuser.com"
        user.save()

        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.tokens.first().key)),
            content_type="application/json", HTTP_ORIGIN="http://localhost:8000"
        )
        self.assertEqual(response.status_code, HTTP_200_OK)

    @override_settings(CUSTOM_AUTH_FLAGS=CUSTOM_AUTH_FLAGS)
    @override_settings(APEX_HOST="http://apex:8000")
    def test_error_apex_user_auth(self):
        user = self.create_test_user()
        user.email = "test.apex_user@testuser.com"
        user.save()

        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.tokens.first().key)),
            content_type="application/json", HTTP_ORIGIN="http://localhost:8000"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_user_enumeration_protection(self):
        email = "test_user@email.com"
        password = "Test_password123!"
        error_message = "That username / password is not valid."
        user = get_user_model().objects.create(email=email)
        user.set_password(password)
        user.save()

        bad_email_response = self.client.post(
            self._url,
            data=json.dumps({
                "username": "a" + email,
                "password": password,
            }),
            content_type="application/json"
        )
        self.assertEqual(bad_email_response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(bad_email_response.data["message"], error_message)

        bad_pass_response = self.client.post(
            self._url,
            data=json.dumps({
                "username": email,
                "password": "a" + password,
            }),
            content_type="application/json"
        )
        self.assertEqual(bad_pass_response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(bad_pass_response.data["message"], error_message)

    def test_logout_unauthenticated_fail(self):
        response = self.client.delete(self._url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_logout_success(self):
        user = self.create_test_user()
        token = UserDeviceToken.objects.filter(user=user).last()
        response = self.client.delete(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertFalse(UserDeviceToken.objects.filter(key=token.key).exists())
