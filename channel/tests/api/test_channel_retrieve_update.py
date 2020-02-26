from unittest.mock import patch

import requests
from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from channel.api.urls.names import ChannelPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models.channel import Channel
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.models import UserChannel
from userprofile.permissions import Permissions
from utils.unittests.celery import mock_send_task
from utils.unittests.es_components_patcher import SearchDSLPatcher
from utils.unittests.int_iterator import int_iterator
from utils.unittests.response import MockResponse
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class ChannelRetrieveUpdateTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, channel_id):
        return reverse(ChannelPathName.CHANNEL, [Namespace.CHANNEL], args=(channel_id,))

    @classmethod
    def setUpClass(cls):
        super(ChannelRetrieveUpdateTestCase, cls).setUpClass()
        Permissions.sync_groups()

    @mock_send_task()
    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_user_can_update_own_channel(self, *args):
        user = self.create_test_user(True)
        channel_id = "test_channel_id"
        UserChannel.objects.create(channel_id=channel_id, user=user)

        with patch("es_components.managers.channel.ChannelManager.get",
                   return_value=[Channel(id=channel_id)]), \
             patch("es_components.managers.channel.ChannelManager.model.get",
                   return_value=Channel(id=channel_id)):
            url = self._get_url(channel_id)
            response = self.client.put(url, dict())

            self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_send_task()
    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_admin_user_can_update_any_channel(self, *args):
        user = self.create_test_user(True)
        user.is_staff = True
        user.save()
        channel_id = "test_channel_id"

        with patch("es_components.managers.channel.ChannelManager.get",
                   return_value=[Channel(id=channel_id)]), \
             patch("es_components.managers.channel.ChannelManager.model.get",
                   return_value=Channel(id=channel_id)):
            url = self._get_url(channel_id)
            response = self.client.put(url, dict())

            self.assertEqual(response.status_code, HTTP_200_OK)

    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_user_can_not_update_not_own_channel(self, *args):
        self.create_test_user(True)
        channel_id = "test_channel_id"

        url = self._get_url(channel_id)
        response = self.client.put(url, dict())

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_enterprise_user_should_be_able_to_see_channel_details(self, *args):
        user = self.create_test_user(True)
        self.fill_all_groups(user)
        channel_id = "test_channel_id"

        with patch("es_components.managers.channel.ChannelManager.model.get",
                   return_value=Channel(id=channel_id)):
            url = self._get_url(channel_id)
            response = self.client.get(url)

            self.assertEqual(response.status_code, HTTP_200_OK)

    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_professional_user_should_see_channel_aw_data(self, *args):
        """
        Ticket https://channelfactory.atlassian.net/browse/SAAS-1695
        """
        user = self.create_test_user(True)
        self.fill_all_groups(user)
        user.refresh_from_db()
        channel_id = "test_channel_id"

        with patch("es_components.managers.channel.ChannelManager.model.get",
                   return_value=Channel(id=channel_id, ads_stats={"clicks_count": 100})):
            url = self._get_url(channel_id)
            response = self.client.get(url)

            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertIn("ads_stats", response.data)

    @mock_send_task()
    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    @patch.object(requests, "put", return_value=MockResponse())
    def test_delete_should_remove_channel_from_channel_list(self, *args):
        user = self.create_test_user(True)
        channel_id = "test_channel_id"
        UserChannel.objects.create(user=user, channel_id=channel_id)
        url = self._get_url(channel_id)
        response = self.client.delete(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertEqual(len(user.channels.all()), 0)

    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_unauths_channel_for_last_user(self, *args):
        user = self.create_test_user(True)
        channel_id = "test_channel_id"
        UserChannel.objects.create(user=user, channel_id=channel_id)
        url = self._get_url(channel_id)
        with patch("channel.api.views.channel_retrieve_update_delete.send_task_delete_channels") as delete_task:
            response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        delete_task.assert_called_once_with(([channel_id],))

    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_unauths_channel_for_not_last_user(self, *args):
        user_1 = self.create_test_user(True)
        user_2 = get_user_model().objects.create(email="test@email.com")
        channel_id = "test_channel_id"
        UserChannel.objects.create(user=user_1, channel_id=channel_id)
        UserChannel.objects.create(user=user_2, channel_id=channel_id)
        url = self._get_url(channel_id)
        with patch("channel.api.views.channel_retrieve_update_delete.send_task_delete_channels") as delete_task:
            response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertFalse(delete_task.called)

    def test_extra_fields(self):
        self.create_admin_user()
        extra_fields = ("brand_safety_data", "chart_data", "blacklist_data")
        channel = Channel(str(next(int_iterator)))
        ChannelManager([Sections.GENERAL_DATA]).upsert([channel])

        url = self._get_url(channel.main.id)
        response = self.client.get(url)

        for field in extra_fields:
            with self.subTest(field):
                self.assertIn(field, response.data)

    def test_ignore_monetization_filter_no_permission(self):
        user = self.create_test_user()
        user.add_custom_user_permission("channel_details")
        channel = Channel(f"test_channel_id_{next(int_iterator)}")
        channel.populate_monetization(is_monetizable=True)
        ChannelManager([Sections.GENERAL_DATA, Sections.AUTH, Sections.MONETIZATION]).upsert([channel])
        url = self._get_url(channel.main.id) + "?fields=main.id%2Cmonetization.is_monetizable"
        response = self.client.get(url)
        self.assertIsNone(response.data.get("monetization"))

    def test_monetization_filter_has_permission(self):
        self.create_admin_user()
        channel = Channel(f"test_channel_id_{next(int_iterator)}")
        channel.populate_monetization(is_monetizable=True)
        ChannelManager([Sections.GENERAL_DATA, Sections.AUTH, Sections.MONETIZATION]).upsert([channel])
        url = self._get_url(channel.main.id) + "?fields=main.id%2Cmonetization.is_monetizable"
        response = self.client.get(url)
        self.assertIsNotNone(response.data.get("monetization"))
