from unittest.mock import patch

from django.core import mail
from rest_framework.reverse import reverse
from rest_framework.status import HTTP_202_ACCEPTED, HTTP_400_BAD_REQUEST

from utils.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher, MockResponse


class ChannelAuthenticationTestCase(ExtendedAPITestCase):
    @patch("channel.api.views.requests")
    def test_success_on_user_duplication(self, requests_mock):
        """
        Bug: https://channelfactory.atlassian.net/browse/SAAS-1602
        On sign in server return server error 500.
        Message:
        NotImplementedError:
        Django doesn't provide a DB representation for AnonymousUser.
        """

        user = self.create_test_user(True)
        url = reverse("channel_api_urls:channel_authentication")
        requests_mock.get.return_value = MockResponse(
            json=dict(email=user.email, image=dict(isDefault=False)))

        with patch("channel.api.views.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url, dict())

        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        data = response.data
        self.assertIn('auth_token', data)

    @patch("singledb.connector.requests")
    def test_proxy_errors_from_sdb(self, requests_mock):
        """
        Bug: https://channelfactory.atlassian.net/browse/SAAS-1718
        Profile Page > Authorize > 408 error when user try
        to Authenticate YT channel on account which doesn't have It
        """
        url = reverse("channel_api_urls:channel_authentication")
        test_error = {"code": "channel_not_found",
                      "detail": "This account doesn't include any channels. "
                                "Please try to authorize other YT channel"}
        requests_mock.post.return_value = MockResponse(
            status_code=HTTP_400_BAD_REQUEST, json=test_error
        )

        response = self.client.post(url, dict(), )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data, test_error)

    @patch("channel.api.views.requests")
    def test_send_welcome_email(self, requests_mock):
        url = reverse("channel_api_urls:channel_authentication")
        user_details = {
            "email": "test@test.test",
            "image": {"isDefault": False},
        }
        requests_mock.get.return_value = MockResponse(json=user_details)

        with patch("channel.api.views.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url, dict(), )

        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        welcome_emails = [m for m in mail.outbox
                          if m.subject.startswith("Welcome")]
        self.assertEqual(len(welcome_emails), 1)
