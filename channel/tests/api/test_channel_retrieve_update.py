import datetime
from unittest.mock import patch

import requests
from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.models import IASHistory
from channel.api.urls.names import ChannelPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models.channel import Channel
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from userprofile.models import UserChannel
from utils.unittests.celery import mock_send_task
from utils.unittests.es_components_patcher import SearchDSLPatcher
from utils.unittests.int_iterator import int_iterator
from utils.unittests.response import MockResponse
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class ChannelRetrieveUpdateTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, channel_id):
        return reverse(ChannelPathName.CHANNEL, [Namespace.CHANNEL], args=(channel_id,))

    @mock_send_task()
    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_user_can_update_own_channel(self, *args):
        user = self.create_test_user(auth=True)
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
        self.create_admin_user(auth=True)
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
        self.create_test_user(auth=True, perms={StaticPermissions.RESEARCH__CHANNEL_DETAIL: False})
        channel_id = "test_channel_id"

        url = self._get_url(channel_id)
        response = self.client.put(url, dict())

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_enterprise_user_should_be_able_to_see_channel_details(self, *args):
        self.create_admin_user(auth=True)
        channel_id = "test_channel_id"

        with patch("es_components.managers.channel.ChannelManager.model.get",
                   return_value=Channel(id=channel_id)):
            url = self._get_url(channel_id)
            response = self.client.get(url)

            self.assertEqual(response.status_code, HTTP_200_OK)

    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_enterprise_user_should_be_able_to_see_chart_data(self, *args):
        self.create_admin_user()
        channel_id = "test_channel_id"
        stats = {
            "subscribers_raw_history": {
                "2020-01-02": 300,
                "2020-01-09": 780
            },
            "views_raw_history": {
                "2020-01-02": 10300,
                "2020-01-09": 30300,
            }
        }
        expected_data = [
            {
                "created_at": "2020-01-02 23:59:59.999999Z",
                "subscribers": 300,
                "views": 10300
            },
            {
                "created_at": "2020-01-09 23:59:59.999999Z",
                "subscribers": 780,
                "views": 30300
            },
        ]

        with patch("es_components.managers.channel.ChannelManager.model.get",
                   return_value=Channel(id=channel_id, stats=stats)):
            url = self._get_url(channel_id)
            response = self.client.get(url)

            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data.get("chart_data"), expected_data)

    @patch("es_components.managers.channel.ChannelManager.search", return_value=SearchDSLPatcher())
    @patch("es_components.managers.channel.ChannelManager.upsert", return_value=None)
    @patch("es_components.managers.video.VideoManager.search", return_value=SearchDSLPatcher())
    def test_professional_user_should_see_channel_aw_data(self, *args):
        """
        Ticket https://channelfactory.atlassian.net/browse/SAAS-1695
        """
        self.create_admin_user()
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
        user = self.create_test_user(auth=True)
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
        user = self.create_test_user(auth=True)
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
        user_1 = self.create_test_user(auth=True)
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

    def test_channel_ias_data(self):
        """ Test that a Channel is serialized with IAS data only if it was included in the latest IAS ingestion """
        self.create_admin_user()
        now = timezone.now()
        channel_manager = ChannelManager((Sections.IAS_DATA, Sections.GENERAL_DATA, Sections.STATS))
        latest_ias = IASHistory.objects.create(name="", started=now, completed=now)
        channel_outdated_ias = Channel(f"channel_{next(int_iterator)}")
        channel_outdated_ias.populate_general_data(title="test")
        channel_outdated_ias.populate_ias_data(ias_verified=now - datetime.timedelta(days=1))
        channel_outdated_ias.populate_stats(total_videos_count=1)

        channel_current_ias = Channel(f"channel_{next(int_iterator)}")
        channel_current_ias.populate_general_data(title="test")
        channel_current_ias.populate_ias_data(ias_verified=latest_ias.started)
        channel_current_ias.populate_stats(total_videos_count=1)

        channel_manager.upsert([channel_outdated_ias, channel_current_ias])

        # Channel should not contain ias_data
        url = self._get_url(channel_outdated_ias.main.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNone(response.data.get("ias_data"))

        # Channel should contain ias_data
        url = self._get_url(channel_current_ias.main.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data.get("ias_data"))
