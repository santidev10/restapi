import json
from unittest.mock import patch

import requests
from django.test import testcases
from rest_framework.status import HTTP_404_NOT_FOUND

from channel.actions import remove_auth_channel
from utils.utils_tests import TestUserMixin, MockResponse
from userprofile.models import UserProfile, UserChannel


class RemoveAuthChannelTestCase(testcases.TestCase, TestUserMixin):
    def test_remove_auth_channel(self):
        user = self.create_test_user()
        with open('saas/fixtures/singledb_channel_list.json') as data_file:
            channels = json.load(data_file)
        channel_id = channels["items"][0]["id"]
        channel = UserChannel.objects.create(channel_id=channel_id, user=user)

        with patch("channel.api.views.channel_set.Connector.delete_channel_test") \
                as delete_mock:
            remove_auth_channel(user.email)

            delete_mock.assert_called_once_with(channel_id)

        self.assertFalse(UserProfile.objects.filter(pk=user.pk).exists())
        self.assertFalse(UserChannel.objects.filter(pk=channel.pk).exists())

    def test_remove_success_on_404_from_sdb(self):
        user = self.create_test_user()
        response = MockResponse(HTTP_404_NOT_FOUND)
        with open('saas/fixtures/singledb_channel_list.json') as data_file:
            channels = json.load(data_file)
        channel_id = channels["items"][0]["id"]
        channel = UserChannel.objects.create(channel_id=channel_id, user=user)

        with patch.object(requests, "delete", return_value=response):
            try:
                remove_auth_channel(user.email)
            except Exception as ex:
                self.fail("Raised {exception}".format(exception=str(ex)))

        self.assertFalse(UserProfile.objects.filter(pk=user.pk).exists())
        self.assertFalse(UserChannel.objects.filter(pk=channel.pk).exists())
