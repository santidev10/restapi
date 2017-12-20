import json
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_202_ACCEPTED, HTTP_200_OK, \
    HTTP_403_FORBIDDEN

from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from userprofile.models import UserChannel


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


class ChannelRetrieveUpdateTestCase(ExtendedAPITestCase):
    def test_user_can_update_own_channel(self):
        user = self.create_test_user(True)
        with open('saas/fixtures/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]
        UserChannel.objects.create(channel_id=channel_id, user=user)

        url = reverse("channel_api_urls:channel",
                      args=(channel_id,))
        with patch("channel.api.views.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(url, dict())

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_admin_user_can_update_any_channel(self):
        user = self.create_test_user(True)
        user.is_staff = True
        user.save()
        with open('saas/fixtures/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]

        url = reverse("channel_api_urls:channel",
                      args=(channel_id,))
        with patch("channel.api.views.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(url, dict())

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_user_can_not_update_not_own_channel(self):
        self.create_test_user(True)
        with open('saas/fixtures/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]

        url = reverse("channel_api_urls:channel",
                      args=(channel_id,))
        with patch("channel.api.views.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(url, dict())

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_enterprise_user_should_be_able_to_see_channel_details(self):
        user = self.create_test_user(True)
        user.set_permissions_from_plan('enterprise')
        user.save()

        with open('saas/fixtures/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]

        url = reverse("channel_api_urls:channel",
                      args=(channel_id,))
        with patch("channel.api.views.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
