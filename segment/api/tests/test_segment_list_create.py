import json
from datetime import date
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_201_CREATED, HTTP_401_UNAUTHORIZED, \
    HTTP_200_OK, HTTP_403_FORBIDDEN

from aw_reporting.models import YTVideoStatistic, AWConnection, \
    AWConnectionToUserRelation, Account, AWAccountPermission, Campaign, AdGroup
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import SegmentChannel
from utils.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher


class SegmentListCreateApiViewTestCase(ExtendedAPITestCase):

    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT + ":" + Name.SEGMENT_LIST,
                       kwargs=dict(segment_type=segment_type))

    def test_create_calculates_ctr_v(self):
        any_date = date(2018, 1, 1)
        user = self.create_test_user()
        manager = Account.objects.create(id=1)
        account = Account.objects.create(id=2)
        account.managers.add(manager)
        account.save()
        connection = AWConnection.objects.create(
            email="email@mail.com", refresh_token="****",
        )
        AWConnectionToUserRelation.objects.create(
            user=user,
            connection=connection,
        )
        AWAccountPermission.objects.get_or_create(
            aw_connection=connection, account=manager,
        )
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign, video_views=1)

        clicks, views = 3, 10
        expected_ctr_v = clicks / views * 100
        test_video_id = "test_video_id"
        YTVideoStatistic.objects.create(
            yt_id=test_video_id, ad_group=ad_group, date=any_date,
            video_views=views, clicks=clicks)

        url = self._get_url("video")
        payload = dict(
            title="Test segment",
            category="private",
            ids_to_add=[test_video_id]
        )
        with patch("segment.models.base.Connector",
                   new=SingleDatabaseApiConnectorPatcher), \
             patch("segment.models.video.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url, json.dumps(payload),
                                        content_type="application/json")

        self.assertEqual(response.status_code, HTTP_201_CREATED)
        aw_data = response.data["adw_data"]
        self.assertIsNotNone(aw_data["ctr_v"])
        self.assertAlmostEqual(aw_data["ctr_v"], expected_ctr_v)

    def test_owner_filter_permissions(self):
        non_exists_user_id = 999999
        response = self.client.get(self._get_url("channel"))
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)
        user = self.create_test_user()
        response = self.client.get(self._get_url("channel"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        response = self.client.get(
            "{}{}{}".format(
                self._get_url("channel"), "?owner_id=", non_exists_user_id))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
        response = self.client.get("{}{}{}".format(
            self._get_url("channel"), "?owner_id=", user.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.is_staff = True
        user.save()
        response = self.client.get("{}{}{}".format(
            self._get_url("channel"), "?owner_id=", non_exists_user_id))
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_owner_filter(self):
        user = self.create_test_user()
        SegmentChannel.objects.create(owner=user)
        SegmentChannel.objects.create()
        expected_segments_count = 1
        response = self.client.get("{}{}{}".format(
            self._get_url("channel"), "?owner_id=", user.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_segments_count)
