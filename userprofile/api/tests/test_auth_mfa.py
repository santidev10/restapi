import json
import os
from unittest.mock import MagicMock

from botocore.exceptions import ClientError
from django.contrib.auth import get_user_model
from django.test import override_settings
from django.utils import timezone
from mock import patch
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from userprofile.models import UserDeviceToken
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase

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


@override_settings(COGNITO_USER_POOL_ID="test-pool")
@override_settings(COGNITO_CLIENT_ID="test-client")
class MFAAuthAPITestCase(ExtendedAPITestCase):
    _url = reverse(UserprofilePathName.AUTH, [Namespace.USER_PROFILE])

    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    def _create_user(self, temp=True, password="1234", with_token=True):
        email = str(next(int_iterator)) + "test@test.com"
        key = "temp_1234" if temp is True else "1234"
        user = get_user_model().objects.create(email=email)
        if with_token:
            token = UserDeviceToken.objects.create(key=key, user=user)
        else:
            token = None
        user.set_password(password)
        user.save()
        user.refresh_from_db()
        return user, token

    def get_admin_respond_to_auth_challenge_mock(self, res):
        mock_client = MagicMock()
        mock_client.admin_respond_to_auth_challenge.return_value = res
        return mock_client

    def test_login_email_password_no_phone_not_verified(self):
        """
        Test username / password login and empty phone_number in response for missing phone numbers
        """
        email = str(next(int_iterator)) + "test@test.com"
        password = "test"
        user = get_user_model().objects.create(
            email=email
        )
        user.set_password(password)
        user.save()
        with patch("userprofile.api.views.user_auth.boto3.client"):
            response = self.client.post(
                self._url, json.dumps(dict(username=email, password=password)),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(response.data["auth_token"].startswith("temp_"))
        self.assertEqual(response.data["username"], user.email)
        self.assertIsNone(response.data.get("phone_number"))

    def test_login_email_password_has_phone_not_verified(self):
        """
        Test username / password login and empty phone_number in response for unverified phone numbers
        """
        email = str(next(int_iterator)) + "test@test.com"
        password = "test"
        user = get_user_model().objects.create(
            email=email,
            phone_number="+19999999",
        )
        user.set_password(password)
        user.save()
        with patch("userprofile.api.views.user_auth.boto3.client"):
            response = self.client.post(
                self._url, json.dumps(dict(username=email, password=password)),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(response.data["auth_token"].startswith("temp_"))
        self.assertEqual(response.data["username"], user.email)
        self.assertIsNone(response.data.get("phone_number"))

    def test_login_email_password_has_phone_verified(self):
        """
        Test username / password login with phone_number in response for verified phone numbers
        """
        email = str(next(int_iterator)) + "test@test.com"
        password = "test"
        user = get_user_model().objects.create(
            email=email,
            phone_number="+19999999",
            phone_number_verified=True,
        )
        user.set_password(password)
        user.save()
        with patch("userprofile.api.views.user_auth.boto3.client"):
            response = self.client.post(
                self._url, json.dumps(dict(username=email, password=password)),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(response.data["auth_token"].startswith("temp_"))
        self.assertEqual(response.data["username"], user.email)
        self.assertEqual(response.data["phone_number"], "**(***)***-" + user.phone_number[-4:])

    def test_login_creates_temp_token(self):
        """
        Test username / password login with temp auth_token in response
        """
        user, _ = self._create_user(with_token=False)
        self.assertFalse(UserDeviceToken.objects.filter(user=user).exists())
        with patch("userprofile.api.views.user_auth.boto3.client"):
            response = self.client.post(
                self._url, json.dumps(dict(username=user.email, password="1234")),
                content_type="application/json",
            )
        created = UserDeviceToken.objects.filter(user=user)
        self.assertTrue(created.exists())
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(response.data["auth_token"].startswith("temp_"))
        self.assertTrue(created[0].key.startswith("temp_"))

    def test_login_token_temp(self):
        """
        Test username / password login removes existing auth_token
        """
        user, token = self._create_user(temp=False)
        self.assertFalse(token.key.startswith("temp_"))
        with patch("userprofile.api.views.user_auth.boto3.client"):
            response = self.client.post(
                self._url, json.dumps(dict(username=user.email, password="1234")),
                content_type="application/json",
            )
        updated = UserDeviceToken.objects.filter(user=user).order_by("created_at").last()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(response.data["auth_token"].startswith("temp_"))
        self.assertTrue(updated.key.startswith("temp_"))

    def test_invalid_token_auth(self):
        """
        Test invalid auth_token login
        """
        self.create_test_user()
        with patch("userprofile.api.views.user_auth.boto3.client"):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token="1234")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_mfa_error_handling_username_exists(self):
        """
        Test handling creating duplicate Cognito user
        """
        _, token = self._create_user()
        mock_client = MagicMock()
        mock_res = {
            "Session": "test_session",
            "ChallengeParameters": {"retries": 5}
        }
        mock_exception = {
            "Error": {
                "Code": "UsernameExistsException"
            }
        }
        mock_client.admin_create_user.side_effect = ClientError(mock_exception, "admin_create_user")
        mock_client.admin_initiate_auth.return_value = mock_res
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, mfa_type="email")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["session"], mock_res["Session"])
        self.assertEqual(response.data["retries"], mock_res["ChallengeParameters"]["retries"])

    def test_mfa_error_handling_update_user(self):
        """
        Test handling trying creating then updating user
        """
        _, token = self._create_user()
        mock_client = MagicMock()
        mock_exception_1 = {
            "Error": {
                "Code": "UsernameExistsException"
            }
        }
        mock_exception_2 = {
            "Error": {
                "Message": "An error occurred.",
                "Code": "Test AWS Code"
            }
        }
        mock_client.admin_create_user.side_effect = ClientError(mock_exception_1, "admin_create_user")
        mock_client.admin_update_user_attributes.side_effect = ClientError(mock_exception_2,
                                                                           "admin_update_user_attributes")
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, mfa_type="email")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["message"], mock_exception_2["Error"]["Message"])

    def test_mfa_start_challenge(self):
        """
        Test response for first step in mfa process
        """
        _, token = self._create_user()
        mock_res = {
            "Session": "test_session",
            "ChallengeParameters": {"retries": 5}
        }
        mock_client = MagicMock()
        mock_client.admin_initiate_auth.return_value = mock_res
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, mfa_type="email")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["session"], mock_res["Session"])
        self.assertEqual(response.data["retries"], mock_res["ChallengeParameters"]["retries"])

    def test_mfa_text_reject_not_verified_phone_number(self):
        """
        Test handle text mfa with non verified phone number
        """
        _, token = self._create_user()
        with patch("userprofile.api.views.user_auth.boto3.client"):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, mfa_type="text")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_mfa_text_success_verified_phone_number(self):
        """
        Test handle text mfa with verified phone number
        """
        user, token = self._create_user()
        user.phone_number = "+19999999"
        user.phone_number_verified = True
        user.save()
        mock_res = {
            "Session": "test_session",
            "ChallengeParameters": {"retries": 5}
        }
        mock_client = MagicMock()
        mock_client.admin_initiate_auth.return_value = mock_res
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, mfa_type="text")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["session"], mock_res["Session"])
        self.assertEqual(response.data["retries"], mock_res["ChallengeParameters"]["retries"])

    def test_mfa_update_verified_phone_number(self):
        """
        Test update phone number with verified phone number
        """
        user, token = self._create_user()
        user.phone_number = "+19999999"
        user.phone_number_verified = True
        user.save()
        mock_res = {
            "Session": "test_session",
            "ChallengeParameters": {"retries": 5}
        }
        mock_client = MagicMock()
        mock_client.admin_initiate_auth.return_value = mock_res
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, mfa_type="email")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["session"], mock_res["Session"])
        self.assertEqual(response.data["retries"], mock_res["ChallengeParameters"]["retries"])

        phone_number_call_arg = mock_client.admin_create_user.call_args[1]["UserAttributes"][1]
        self.assertEqual(phone_number_call_arg["Name"], "phone_number")
        self.assertEqual(phone_number_call_arg["Value"], user.phone_number)

    def test_mfa_retries_exceeded(self):
        """
        Test reject after max tries exceeded
        """
        user, token = self._create_user()
        session = "test_session"
        retries = 0
        mock_res = {
            "Session": session,
            "ChallengeParameters": {"retries": retries}
        }
        mock_client = self.get_admin_respond_to_auth_challenge_mock(mock_res)
        mock_exception = {
            "Error": {
                "Code": "NotAuthorizedException",
                "Message": "Incorrect username or password."
            }
        }
        mock_client.admin_respond_to_auth_challenge.side_effect = ClientError(mock_exception,
                                                                              "admin_respond_to_auth_challenge")
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, session=session, answer="test_answer")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertFalse(UserDeviceToken.objects.filter(user=user).exists())
        self.assertEqual(str(response.data["message"]), "Max attempts exceeded. Please log in again.")

    def test_mfa_submit_challenge_failure(self):
        """
        Test handle incorrect mfa answer
        """
        _, token = self._create_user()
        session = "test_session"
        retries = 4
        mock_res = {
            "Session": session,
            "ChallengeParameters": {"retries": retries}
        }
        mock_client = self.get_admin_respond_to_auth_challenge_mock(mock_res)
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, session=session, answer="test_answer")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["session"], session)
        self.assertEqual(response.data["retries"], retries)

    def test_mfa_submit_challenge_session_invalid(self):
        """
        Test handle invalid session
        """
        user, token = self._create_user()
        session = "test_session"
        retries = 4
        mock_res = {
            "Session": session,
            "ChallengeParameters": {"retries": retries}
        }
        mock_client = self.get_admin_respond_to_auth_challenge_mock(mock_res)
        mock_exception = {
            "Error": {
                "Message": "Invalid session for the user.",
                "Code": ""
            }
        }
        mock_client.admin_respond_to_auth_challenge.side_effect = ClientError(mock_exception,
                                                                              "admin_respond_to_auth_challenge")
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, session=session, answer="test_answer")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertFalse(UserDeviceToken.objects.filter(user=user).exists())
        self.assertEqual(str(response.data["message"]), "Invalid session for the user. Please log in again.")

    def test_mfa_submit_challenge_success(self):
        """
        Test handle successful mfa login
        """
        user, token = self._create_user()
        mock_res = {
            "AuthenticationResult": {
                "AccessToken": "test_token"
            }
        }
        mock_client = MagicMock()
        mock_client.admin_respond_to_auth_challenge.return_value = mock_res
        before = timezone.now()
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, session="temp_session", answer="test_answer")),
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
                "role_id",
                "token",
                "is_active",
                "has_accepted_GDPR",
                "user_type",
            }
        )
        updated = UserDeviceToken.objects.get(user=user)
        self.assertFalse(updated.key.startswith("temp_"))
        self.assertFalse(response.data["token"].startswith("temp_"))
        self.assertTrue(updated.created_at > before)

    def test_google_token_resets_auth_token(self):
        """
        Test google oauth login resets auth_token
        """
        user, token = self._create_user()
        self.assertTrue(token.key.startswith("temp_"))
        before = timezone.now()
        with patch("userprofile.api.views.user_auth.UserAuthApiView.get_google_user", return_value=user):
            response = self.client.post(
                self._url, json.dumps(dict(token="test_google_token")),
                content_type="application/json",
            )
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
                "role_id",
                "is_active",
                "has_accepted_GDPR",
                "user_type",
                "token",
            }
        )
        updated = UserDeviceToken.objects.get(key=response.data["token"])
        self.assertFalse(updated.key.startswith("temp_"))
        self.assertTrue(updated.created_at > before)

    def multiple_sign_in_multiple_tokens(self):
        """
        Test allow multiple auth_tokens per user (multi device login)
        """
        email = str(next(int_iterator)) + "test@test.com"
        password = "test"
        user = get_user_model().objects.create(
            email=email
        )
        user.set_password(password)
        user.save()
        with patch("userprofile.api.views.user_auth.boto3.client"):
            response_1 = self.client.post(
                self._url, json.dumps(dict(username=email, password=password)),
                content_type="application/json",
            )
        self.assertEqual(response_1.status_code, HTTP_200_OK)
        self.assertTrue(response_1.data["auth_token"].startswith("temp_"))
        self.assertEqual(response_1.data["username"], user.email)

        with patch("userprofile.api.views.user_auth.boto3.client"):
            response_2 = self.client.post(
                self._url, json.dumps(dict(username=email, password=password)),
                content_type="application/json",
            )
        self.assertEqual(response_2.status_code, HTTP_200_OK)
        self.assertTrue(response_2.data["auth_token"].startswith("temp_"))
        self.assertEqual(response_2.data["username"], user.email)

        self.assertTrue(UserDeviceToken.objects.filter(user=user, key=response_1.data["auth_token"]).exists())
        self.assertTrue(UserDeviceToken.objects.filter(user=user, key=response_2.data["auth_token"]).exists())
