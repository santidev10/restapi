import json
from datetime import date
from unittest.mock import patch

from django.core.urlresolvers import reverse
from django.http import QueryDict
from rest_framework.status import HTTP_201_CREATED, HTTP_401_UNAUTHORIZED, \
    HTTP_200_OK, HTTP_400_BAD_REQUEST

from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.models import YTVideoStatistic, AWConnection, \
    Account, AWAccountPermission, Campaign, AdGroup
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import SegmentChannel
from segment.models import SegmentVideo
from utils.utittests.int_iterator import int_iterator
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentListCreateApiViewTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT + ":" + Name.SEGMENT_LIST,
                       kwargs=dict(segment_type=segment_type))

    def test_create_calculates_ctr_v(self):
        any_date = date(2018, 1, 1)
        self.create_test_user()
        manager = Account.objects.create(id=load_web_app_settings()["cf_account_id"])
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(manager)
        account.save()
        connection = AWConnection.objects.create(
            email="email@mail.com", refresh_token="****",
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
        aw_data = response.data["adw_data"]["stats"]
        self.assertIsNotNone(aw_data["ctr_v"])
        self.assertAlmostEqual(aw_data["ctr_v"], expected_ctr_v)

    def test_get_list_unauthorized(self):
        response = self.client.get(self._get_url("channel"))
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_get_list_authorized(self):
        self.create_test_user()
        response = self.client.get(self._get_url("channel"))
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_owner_filter_bad_request(self):
        non_exists_user_id = 999999
        self.create_test_user()
        query_prams = QueryDict(
            "owner_id={}".format(non_exists_user_id)).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("channel"), query_prams))
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_owner_filter_success_not_admin(self):
        user = self.create_test_user()
        query_prams = QueryDict("owner_id={}".format(user.id)).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("channel"), query_prams))
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_owner_filter_success_admin(self):
        non_exists_user_id = 999999
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        query_prams = QueryDict(
            "owner_id={}".format(non_exists_user_id)).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("channel"), query_prams))
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_owner_filter(self):
        user = self.create_test_user()
        owned_segment = SegmentChannel.objects.create(owner=user)
        SegmentChannel.objects.create()
        expected_segments_count = 1
        query_prams = QueryDict("owner_id={}".format(user.id)).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("channel"), query_prams))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_segments_count)
        self.assertEqual(
            response.data["items"][0]["id"], owned_segment.id)

    def test_create_channel_segment_from_filters(self):
        self.create_test_user()
        manager = Account.objects.create(id=load_web_app_settings()["cf_account_id"])
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(manager)
        account.save()
        connection = AWConnection.objects.create(
            email="email@mail.com", refresh_token="****",
        )
        AWAccountPermission.objects.get_or_create(
            aw_connection=connection, account=manager,
        )
        campaign = Campaign.objects.create(account=account)
        AdGroup.objects.create(campaign=campaign, video_views=1)

        url = self._get_url("channel")
        test_filters = dict(
            verified__term=True,
            gender=[
                {
                    "id": "_female__range",
                    "value": 0
                }
            ],
        )
        payload = dict(
            title="Test segment",
            category="private",
            filters=test_filters
        )
        batches = [
            [dict(pk="1")],
            [dict(pk="2")],
        ]

        sdb_generator = (batch for batch in batches)

        with patch("segment.models.base.Connector", new=SingleDatabaseApiConnectorPatcher), \
             patch("segment.models.channel.Connector", new=SingleDatabaseApiConnectorPatcher), \
             patch.object(SingleDatabaseApiConnectorPatcher, "get_channel_list_full",
                          return_value=sdb_generator) as mock:
            response = self.client.post(url, json.dumps(payload),
                                        content_type="application/json")

        self.assertEqual(response.status_code, HTTP_201_CREATED)
        mock.assert_called_with(test_filters, fields=ListEqualInclude(["pk"]))
        segment_id = response.data["id"]
        segment = SegmentChannel.objects.get(pk=segment_id)
        self.assertEqual(len(segment.related.all()), 2)

    def test_create_video_segment_from_filters(self):
        self.create_test_user()
        manager = Account.objects.create(id=load_web_app_settings()["cf_account_id"])
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(manager)
        account.save()
        connection = AWConnection.objects.create(
            email="email@mail.com", refresh_token="****",
        )
        AWAccountPermission.objects.get_or_create(
            aw_connection=connection, account=manager,
        )
        campaign = Campaign.objects.create(account=account)
        AdGroup.objects.create(campaign=campaign, video_views=1)

        url = self._get_url("video")
        test_filters = dict(
            verified__term=True,
            gender=[
                {
                    "id": "_female__range",
                    "value": 0
                }
            ],
        )
        payload = dict(
            title="Test segment",
            category="private",
            filters=test_filters
        )
        batches = [
            [dict(pk="1")],
            [dict(pk="2")],
        ]
        sdb_generator = (batch for batch in batches)

        with patch("segment.models.base.Connector", new=SingleDatabaseApiConnectorPatcher), \
             patch("segment.models.video.Connector", new=SingleDatabaseApiConnectorPatcher), \
             patch.object(SingleDatabaseApiConnectorPatcher, "get_video_list_full",
                          return_value=sdb_generator) as mock:
            response = self.client.post(url, json.dumps(payload),
                                        content_type="application/json")

        self.assertEqual(response.status_code, HTTP_201_CREATED)
        mock.assert_called_with(test_filters, fields=ListEqualInclude(["pk"]))
        segment_id = response.data["id"]
        segment = SegmentVideo.objects.get(pk=segment_id)
        self.assertEqual(len(segment.related.all()), 2)


class ListEqualInclude(list):
    def __eq__(self, other):
        missed_values = set(self) - set(other)
        return len(missed_values) == 0

    def __ne__(self, other):
        return False

    def __repr__(self):
        return "<ListEqualInclude {}>".format(list(self))
