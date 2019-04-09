from unittest.mock import patch

from django.core import mail
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST

import singledb.connector
from channel.api.urls.names import ChannelPathName
from saas.urls.namespaces import Namespace
from utils.utittests.response import MockResponse
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class ChannelAuthenticationTestCase(ExtendedAPITestCase):
    url = reverse(ChannelPathName.CHANNEL_AUTHENTICATION, [Namespace.CHANNEL])

    @patch("channel.api.views.channel_authentication.requests")
    def test_success_on_user_duplication(self, requests_mock):
        """
        Bug: https://channelfactory.atlassian.net/browse/SAAS-1602
        On sign in server return server error 500.
        Message:
        NotImplementedError:
        Django doesn't provide a DB representation for AnonymousUser.
        """

        user = self.create_test_user(True)
        requests_mock.get.return_value = MockResponse(
            json=dict(email=user.email, image=dict(isDefault=False)))

        response = self.client.post(self.url, dict())

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
        test_error = {
            "code": "channel_not_found",
            "detail": "This account doesn't include any channels. "
                      "Please try to authorize other YT channel"
        }
        requests_mock.post.return_value = MockResponse(
            status_code=HTTP_400_BAD_REQUEST, json=test_error
        )

        with patch("channel.api.views.channel_authentication.Connector",
                   new=singledb.connector.SingleDatabaseApiConnector_origin):
            response = self.client.post(self.url, dict(), )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data, test_error)

    @patch("channel.api.views.channel_authentication.requests")
    def test_send_welcome_email(self, requests_mock):
        user_details = {
            "email": "test@test.test",
            "image": {"isDefault": False},
        }
        requests_mock.get.return_value = MockResponse(json=user_details)

        response = self.client.post(self.url, dict(), )

        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        welcome_emails = [m for m in mail.outbox
                          if m.subject.startswith("Welcome")]
        self.assertEqual(len(welcome_emails), 1)
