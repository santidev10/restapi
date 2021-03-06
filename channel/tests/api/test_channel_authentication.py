from unittest.mock import patch

from django.core import mail
from django.utils import timezone
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST

from channel.api.urls.names import ChannelPathName
from channel.models import AuthChannel
from es_components.datetime_service import datetime_service
from es_components.models.channel import Channel
from saas.urls.namespaces import Namespace
from userprofile.constants import DEFAULT_DOMAIN
from userprofile.constants import StaticPermissions
from userprofile.models import PermissionItem
from userprofile.models import UserDeviceToken
from userprofile.models import UserProfile
from userprofile.models import WhiteLabel
from utils.unittests.celery import mock_send_task
from utils.unittests.response import MockResponse
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class ChannelAuthenticationTestCase(ExtendedAPITestCase):
    url = reverse(ChannelPathName.CHANNEL_AUTHENTICATION, [Namespace.CHANNEL])

    @mock_send_task()
    @patch("channel.api.views.channel_authentication.ChannelManager.get_or_create",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.get",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.upsert")
    @patch("channel.api.views.channel_authentication.requests")
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    @patch("channel.api.views.channel_authentication.YoutubeAPIConnector")
    def test_success_on_user_duplication(self, mock_youtube, flow, requests_mock, *args):
        """
        Bug: https://channelfactory.atlassian.net/browse/SAAS-1602
        On sign in server return server error 500.
        Message:
        NotImplementedError:
        Django doesn"t provide a DB representation for AnonymousUser.
        """

        user = self.create_test_user(auth=True)

        youtube_own_channel_test_value = {"items": [{"id": "channel_id"}]}
        requests_mock.get.return_value = MockResponse(
            json=dict(email=user.email, image=dict(isDefault=False)))

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime_service.now()

        mock_youtube().own_channels.return_value = youtube_own_channel_test_value

        response = self.client.post(self.url, dict(code="code"))

        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        data = response.data
        self.assertIn("auth_token", data)
        self.assertEqual(len(mail.outbox), 2)

    @mock_send_task()
    @patch("channel.api.views.channel_authentication.ChannelManager.get_or_create",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.get",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.upsert")
    @patch("channel.api.views.channel_authentication.requests")
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    @patch("channel.api.views.channel_authentication.YoutubeAPIConnector")
    def test_reauthentification(self, mock_youtube, flow, requests_mock, *args):
        """
        Bug: https://channelfactory.atlassian.net/browse/SAAS-1602
        On sign in server return server error 500.
        Message:
        NotImplementedError:
        Django doesn"t provide a DB representation for AnonymousUser.
        """

        user = self.create_test_user(auth=True)
        AuthChannel.objects.create(
            channel_id="channel_id",
            refresh_token="^test_refresh_token$",
            access_token="^test_access_token$",
            access_token_expire_at=datetime_service.now(),
            token_revocation=datetime_service.now()
        )

        youtube_own_channel_test_value = {"items": [{"id": "channel_id"}]}
        requests_mock.get.return_value = MockResponse(
            json=dict(email=user.email, image=dict(isDefault=False)))

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime_service.now()

        mock_youtube().own_channels.return_value = youtube_own_channel_test_value

        response = self.client.post(self.url, dict(code="code"))

        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        data = response.data
        self.assertIn("auth_token", data)
        self.assertEqual(len(mail.outbox), 1)

        auth_channel = AuthChannel.objects.filter(channel_id="channel_id").first()
        self.assertIsNone(auth_channel.token_revocation)

    def test_error_no_code(self):
        response = self.client.post(self.url, dict())
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    @patch("channel.api.views.channel_authentication.ChannelManager.get_or_create",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.get",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.upsert")
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    @patch("channel.api.views.channel_authentication.YoutubeAPIConnector")
    def test_proxy_errors_from_sdb(self, mock_youtube, flow, *args):
        """
        Bug: https://channelfactory.atlassian.net/browse/SAAS-1718
        Profile Page > Authorize > 408 error when user try
        to Authenticate YT channel on account which doesn"t have It
        """
        test_error = {
            "detail": "This account doesn't include any channels. "
                      "Please try to authorize another YouTube account with channels."
        }

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime_service.now()

        mock_youtube().own_channels.return_value = {"items": []}

        response = self.client.post(self.url, dict(code="code"))

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data, test_error)

    @mock_send_task()
    @patch("channel.api.views.channel_authentication.ChannelManager.get_or_create",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.get",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.upsert")
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    @patch("channel.api.views.channel_authentication.YoutubeAPIConnector")
    def test_send_welcome_email(self, mock_youtube, flow, *args):
        user_details = {
            "email": "test@test.test",
            "image": {"isDefault": False},
        }
        youtube_own_channel_test_value = {"items": [{"id": "channel_id"}]}

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime_service.now()

        mock_youtube().own_channels.return_value = youtube_own_channel_test_value

        with patch("channel.api.views.channel_authentication.requests.get",
                   return_value=MockResponse(json=user_details)):
            response = self.client.post(self.url, dict(code="code"), )

            self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
            welcome_emails = [m for m in mail.outbox if m.subject.startswith("Welcome")]
            self.assertEqual(len(welcome_emails), 1)

    @mock_send_task()
    @patch("channel.api.views.channel_authentication.ChannelManager.get_or_create",
           return_value=[Channel("channel_id_test")])
    @patch("channel.api.views.channel_authentication.ChannelManager.get",
           return_value=[Channel("channel_id_test")])
    @patch("channel.api.views.channel_authentication.ChannelManager.upsert")
    @patch("channel.api.views.channel_authentication.requests")
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    @patch("channel.api.views.channel_authentication.YoutubeAPIConnector")
    def test_success_set_auth_token(self, mock_youtube, flow, requests_mock, *args):
        user_details = {
            "email": "tester@test.test",
            "image": {"isDefault": False},
        }
        user = UserProfile.objects.create(email=user_details["email"])
        user.refresh_from_db()
        before = timezone.now()

        youtube_own_channel_test_value = {"items": [{"id": "channel_id"}]}

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime_service.now()

        mock_youtube().own_channels.return_value = youtube_own_channel_test_value

        with patch("channel.api.views.channel_authentication.requests.get",
                   return_value=MockResponse(json=user_details)):
            response = self.client.post(self.url, dict(code="code"))

        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        data = response.data
        self.assertIn("auth_token", data)
        self.assertFalse(data["auth_token"].startswith("temp_"))
        device_auth_token = UserDeviceToken.objects.get(user=user, key=data["auth_token"])
        self.assertFalse(device_auth_token.key.startswith("temp_"))
        self.assertTrue(device_auth_token.created_at > before)

    @mock_send_task()
    @patch("channel.api.views.channel_authentication.ChannelManager.get_or_create",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.get",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.upsert")
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    @patch("channel.api.views.channel_authentication.YoutubeAPIConnector")
    def test_domain_set(self, mock_youtube, flow, *args):
        """ Test that domain foriegn key is set correctly when creating user through channel authentication """
        host_domain, _ = WhiteLabel.objects.get_or_create(domain="rc")
        user_details = {
            "email": "test_domain@test.test",
            "image": {"isDefault": False},
        }
        youtube_own_channel_test_value = {"items": [{"id": "channel_id"}]}

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime_service.now()

        mock_youtube().own_channels.return_value = youtube_own_channel_test_value

        with patch("channel.api.views.channel_authentication.requests.get",
                   return_value=MockResponse(json=user_details)),\
                patch.object(WhiteLabel, "extract_sub_domain", return_value=host_domain.domain):
            response = self.client.post(self.url, dict(code="code"), )
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        created_user = UserProfile.objects.get(email=user_details["email"])
        self.assertNotEqual(created_user.domain.domain, DEFAULT_DOMAIN)
        self.assertEqual(created_user.domain.domain, host_domain.domain)

    @mock_send_task()
    @patch("channel.api.views.channel_authentication.ChannelManager.get_or_create",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.get",
           return_value=[Channel("channel_id")])
    @patch("channel.api.views.channel_authentication.ChannelManager.upsert")
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    @patch("channel.api.views.channel_authentication.YoutubeAPIConnector")
    def test_new_user_permissions(self, mock_youtube, flow, *args):
        """ Test that Managed Service Permissions are disabled by default through Google OAuth """
        user_details = {
            "email": "test_new_user_permissions@test.test",
            "image": {"isDefault": False},
        }
        youtube_own_channel_test_value = {"items": [{"id": "channel_id"}]}

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime_service.now()

        mock_youtube().own_channels.return_value = youtube_own_channel_test_value

        with patch("channel.api.views.channel_authentication.requests.get",
                   return_value=MockResponse(json=user_details)):
            response = self.client.post(self.url, dict(code="code"), )
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        disabled_managed_service_perms = [perm_name for perm_name in PermissionItem.all_perms()
                                          if StaticPermissions.MANAGED_SERVICE in perm_name]
        user = UserProfile.objects.get(email=user_details["email"])
        expected_vals = {
            perm_name: False for perm_name in disabled_managed_service_perms
        }
        actual_vals = {
            perm_name: user.perms[perm_name] for perm_name in disabled_managed_service_perms
        }
        self.assertEqual(expected_vals, actual_vals)