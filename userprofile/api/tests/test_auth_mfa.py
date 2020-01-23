import json
from unittest.mock import MagicMock
from mock import patch
import os

from botocore.exceptions import ClientError
from django.contrib.auth import get_user_model
from django.test import override_settings
from rest_framework.authtoken.models import Token
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from utils.utittests.reverse import reverse
from utils.utittests.int_iterator import int_iterator
from utils.utittests.test_case import ExtendedAPITestCase


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

    def create_user_with_temp_token(self):
        email = str(next(int_iterator)) + "test@test.com"
        key = "temp_1234"
        user = get_user_model().objects.create(email=email)
        token = Token.objects.create(key=key, user=user)
        user.auth_token = token
        user.save()
        return user, token

    def get_admin_respond_to_auth_challenge_mock(self, res):
        mock_client = MagicMock()
        mock_client.admin_respond_to_auth_challenge.return_value = res
        return mock_client

    def test_login_email_password_no_phone(self):
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

    def test_login_email_password_has_phone(self):
        email = str(next(int_iterator)) + "test@test.com"
        password = "test"
        user = get_user_model().objects.create(
            email=email,
            phone_number="+19999999"
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

    def test_invalid_token_auth(self):
        self.create_test_user()
        with patch("userprofile.api.views.user_auth.boto3.client"):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token="1234")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_mfa_start_challenge(self):
        user, token = self.create_user_with_temp_token()
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

    def test_mfa_retries_exceeded(self):
        user, token = self.create_user_with_temp_token()
        session = "test_session"
        retries = 0
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
        self.assertFalse(Token.objects.filter(user=user).exists())
        self.assertEqual(str(response.data["message"]), "Retries exceeded. Please log in again.")

    def test_mfa_submit_challenge_failure(self):
        user, token = self.create_user_with_temp_token()
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
        user, token = self.create_user_with_temp_token()
        session = "test_session"
        retries = 4
        mock_res = {
            "Session": session,
            "ChallengeParameters": {"retries": retries}
        }
        mock_client = self.get_admin_respond_to_auth_challenge_mock(mock_res)
        mock_exception = {
            "Error": {
                "Message": "Invalid session for the user."
            }
        }
        mock_client.admin_respond_to_auth_challenge.side_effect = ClientError(mock_exception, "admin_respond_to_auth_challenge")
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, session=session, answer="test_answer")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(str(response.data["message"]), "Invalid session for the user. Please request a new login code.")

    def test_mfa_submit_challenge_success(self):
        user, token = self.create_user_with_temp_token()
        mock_res = {
            "AuthenticationResult": {
                "AccessToken": "test_token"
            }
        }
        mock_client = MagicMock()
        mock_client.admin_respond_to_auth_challenge.return_value = mock_res
        with patch("userprofile.api.views.user_auth.boto3.client", return_value=mock_client):
            response = self.client.post(
                self._url, json.dumps(dict(auth_token=token.key, session="temp_session", answer="test_answer")),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data),
            {
                "access",
                "aw_settings",
                "can_access_media_buying",
                "company",
                "date_joined",
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
                "phone_number",
                "token",
                "is_active",
                "has_accepted_GDPR",
                "user_type",
            }
        )
