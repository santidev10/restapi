import json
from unittest.mock import patch

import requests
from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from channel.api.urls.names import ChannelPathName
from saas.urls.namespaces import Namespace
from singledb.connector import SingleDatabaseApiConnector
from userprofile.models import UserChannel
from userprofile.permissions import Permissions
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.reverse import reverse
from utils.utittests.response import MockResponse
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher


class ChannelRetrieveUpdateTestCase(ExtendedAPITestCase):
    def _get_url(self, channel_id):
        return reverse(ChannelPathName.CHANNEL, [Namespace.CHANNEL], args=(channel_id,))

    @classmethod
    def setUpClass(cls):
        super(ChannelRetrieveUpdateTestCase, cls).setUpClass()
        Permissions.sync_groups()

    def test_user_can_update_own_channel(self):
        user = self.create_test_user(True)
        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]
        UserChannel.objects.create(channel_id=channel_id, user=user)

        url = self._get_url(channel_id)
        with patch("channel.api.views.channel_retrieve_update_delete.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(url, dict())

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_admin_user_can_update_any_channel(self):
        user = self.create_test_user(True)
        user.is_staff = True
        user.save()
        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]

        url = self._get_url(channel_id)
        with patch("channel.api.views.channel_retrieve_update_delete.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(url, dict())

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_user_can_not_update_not_own_channel(self):
        self.create_test_user(True)
        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]

        url = self._get_url(channel_id)
        with patch("channel.api.views.channel_retrieve_update_delete.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(url, dict())

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_enterprise_user_should_be_able_to_see_channel_details(self):
        user = self.create_test_user(True)
        self.fill_all_groups(user)

        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]

        url = self._get_url(channel_id)
        with patch("channel.api.views.channel_retrieve_update_delete.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_professional_user_should_see_channel_aw_data(self):
        """
        Ticket https://channelfactory.atlassian.net/browse/SAAS-1695
        """
        user = self.create_test_user(True)
        self.fill_all_groups(user)
        user.refresh_from_db()
        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]

        url = self._get_url(channel_id)
        with patch("channel.api.views.channel_retrieve_update_delete.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("aw_data", response.data)

    @patch.object(requests, "put", return_value=MockResponse())
    def test_delete_should_remove_channel_from_channel_list(self, put_mock):
        user = self.create_test_user(True)
        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]
        UserChannel.objects.create(user=user, channel_id=channel_id)
        url = self._get_url(channel_id)
        response = self.client.delete(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertEqual(len(user.channels.all()), 0)

    @patch.object(requests, "put")
    @patch.object(SingleDatabaseApiConnector, "unauthorize_channel")
    def test_unauths_channel_for_last_user(self, unauth_mock, put_mock):
        user = self.create_test_user(True)
        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]
        UserChannel.objects.create(user=user, channel_id=channel_id)
        url = self._get_url(channel_id)
        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        unauth_mock.assert_called_once_with(channel_id)

    @patch.object(SingleDatabaseApiConnector, "unauthorize_channel")
    def test_unauths_channel_for_not_last_user(self, unauth_mock):
        user_1 = self.create_test_user(True)
        user_2 = get_user_model().objects.create(email="test@email.com")
        with open('saas/fixtures/tests/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        channel_id = data["items"][0]["id"]
        UserChannel.objects.create(user=user_1, channel_id=channel_id)
        UserChannel.objects.create(user=user_2, channel_id=channel_id)
        url = self._get_url(channel_id)
        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertFalse(unauth_mock.called)
