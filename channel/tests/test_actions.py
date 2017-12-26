import json
from unittest.mock import patch

from django.test import testcases

from channel.actions import remove_auth_channel
from saas.utils_tests import TestUserMixin
from userprofile.models import UserProfile, UserChannel


class RemoveAuthChannelTestCase(testcases.TestCase, TestUserMixin):
    @patch("channel.api.views.requests")
    def test_remove_auth_channel(self, requests_mock):
        user = self.create_test_user(True)
        # # requests_mock.get.return_value = MockResponse(
        # #     dict(email=user.email, image=dict(isDefault=False)))
        with open('saas/fixtures/singledb_channel_list.json') as data_file:
            channels = json.load(data_file)
        channel_id = channels["items"][0]["id"]
        channel = UserChannel.objects.create(channel_id=channel_id, user=user)

        with patch(
                "channel.api.views.Connector.delete_channel") as delete_mock:
            remove_auth_channel(user.email)

            delete_mock.assert_called_once_with(channel_id)

        self.assertFalse(UserProfile.objects.filter(pk=user.pk).exists())
        self.assertFalse(UserChannel.objects.filter(pk=channel.pk).exists())
