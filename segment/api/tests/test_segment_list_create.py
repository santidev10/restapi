import json
from datetime import date
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_201_CREATED

from aw_reporting.models import YTVideoStatistic, AWConnection, \
    AWConnectionToUserRelation, Account, AWAccountPermission, Campaign, AdGroup
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
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
