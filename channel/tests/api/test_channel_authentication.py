from unittest.mock import patch

from datetime import datetime

from django.core import mail
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST

from channel.api.urls.names import ChannelPathName
from saas.urls.namespaces import Namespace
from utils.utittests.response import MockResponse
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class ChannelAuthenticationTestCase(ExtendedAPITestCase):
    url = reverse(ChannelPathName.CHANNEL_AUTHENTICATION, [Namespace.CHANNEL])

    @patch("es_components.managers.video.VideoManager.get_all_video_ids", return_value=[])
    @patch("channel.api.views.channel_authentication.requests")
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    def test_success_on_user_duplication(self, flow, requests_mock, *args):
        """
        Bug: https://channelfactory.atlassian.net/browse/SAAS-1602
        On sign in server return server error 500.
        Message:
        NotImplementedError:
        Django doesn't provide a DB representation for AnonymousUser.
        """

        user = self.create_test_user(True)

        youtube_own_channel_test_value = {"items": [{"id": "channel_id"}]}
        requests_mock.get.return_value = MockResponse(
            json=dict(email=user.email, image=dict(isDefault=False)))

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime.now()

        with patch("utils.youtube_api.YoutubeAPIConnector.own_channels",
                      return_value=youtube_own_channel_test_value):

            response = self.client.post(self.url, dict(code="code"))

            self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
            data = response.data
            self.assertIn('auth_token', data)

    def test_error_no_code(self):
        response = self.client.post(self.url, dict())
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    @patch("utils.youtube_api.YoutubeAPIConnector.own_channels", return_value={"items": []})
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    def test_proxy_errors_from_sdb(self, flow, *args):
        """
        Bug: https://channelfactory.atlassian.net/browse/SAAS-1718
        Profile Page > Authorize > 408 error when user try
        to Authenticate YT channel on account which doesn't have It
        """
        test_error = {
            "detail": "This account doesn't include any channels. "
                      "Please try to authorize other YT channel"
        }

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime.now()

        response = self.client.post(self.url, dict(code="code"))

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data, test_error)

    @patch("es_components.managers.video.VideoManager.get_all_video_ids", return_value=[])
    @patch("channel.api.views.channel_authentication.OAuth2WebServerFlow")
    def test_send_welcome_email(self, flow, *args):
        user_details = {
            "email": "test@test.test",
            "image": {"isDefault": False},
        }
        youtube_own_channel_test_value = {"items": [{"id": "channel_id"}]}

        flow().step2_exchange().refresh_token = "^test_refresh_token$"
        flow().step2_exchange().access_token = "^test_access_token$"
        flow().step2_exchange().token_expiry = datetime.now()

        with patch("channel.api.views.channel_authentication.requests.get",
                   return_value=MockResponse(json=user_details)), \
             patch("utils.youtube_api.YoutubeAPIConnector.own_channels", return_value=youtube_own_channel_test_value):

            response = self.client.post(self.url, dict(code="code"), )

            self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
            welcome_emails = [m for m in mail.outbox
                          if m.subject.startswith("Welcome")]
            self.assertEqual(len(welcome_emails), 1)
