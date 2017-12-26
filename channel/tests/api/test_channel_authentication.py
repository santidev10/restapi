from unittest.mock import patch

from rest_framework.reverse import reverse
from rest_framework.status import HTTP_202_ACCEPTED

from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher


class MockResponse(object):
    def __init__(self, json=None):
        self.status_code = 200
        self._json = json

    def json(self):
        return self._json


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
            dict(email=user.email, image=dict(isDefault=False)))

        with patch("channel.api.views.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url, dict())

        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        data = response.data
        self.assertIn('auth_token', data)
