import csv
from datetime import datetime
from time import sleep

import pytz
from django.test import override_settings
from elasticsearch_dsl import Double
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from channel.api.urls.names import ChannelPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.models import Keyword
from es_components.models.base import BaseDocument
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class ChannelListExportTestCase(ExtendedAPITestCase, ESTestCase):
    def _request(self):
        url = reverse(
            ChannelPathName.CHANNEL_LIST_EXPORT,
            [Namespace.CHANNEL],
        )
        return self.client.get(url)

    def test_not_auth(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_no_permissions(self):
        self.create_test_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_admin(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_allowed_user(self):
        user = self.create_test_user()
        user.add_custom_user_permission("channel_list")

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_csv_response(self):
        test_datetime = datetime(2020, 3, 4, 5, 6, 7)
        self.create_admin_user()

        with patch_now(test_datetime):
            response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "text/csv")
        expected_filename = "Channels export report {}.csv".format(test_datetime.strftime("%Y-%m-%d_%H-%m"))
        self.assertEqual(response["Content-Disposition"], "attachment; filename=\"{}\"".format(expected_filename))

    def test_headers(self):
        self.create_admin_user()

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "title",
            "url",
            "country",
            "category",
            "emails",
            "subscribers",
            "thirty_days_subscribers",
            "thirty_days_views",
            "views_per_video",
            "sentiment",
            "engage_rate",
            "last_video_published_at",
            "brand_safety_score",
            "video_view_rate",
            "ctr",
            "ctr_v",
            "average_cpv",
        ])

    def test_response(self):
        self.create_admin_user()
        channel = Channel(next(int_iterator))
        channel.populate_general_data(
            title="Test channel title",
            country="Test country",
            top_category="Top category",
            emails=["example1@mail.com", "example2@email.com"],
        )
        channel.populate_stats(
            subscribers=123,
            last_30day_subscribers=12,
            last_30day_views=321,
            views_per_video=123.4,
            sentiment=0.23,
            engage_rate=0.34,
            last_video_published_at=datetime(2018, 2, 3, 4, 5, 6, tzinfo=pytz.utc),
        )
        channel.populate_ads_stats(
            video_view_rate=12.3,
            ctr=12.3,
            ctr_v=23.4,
            average_cpv=34.5,
        )
        manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS))
        manager.upsert([channel])

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        expected_values = [
            channel.general_data.title,
            f"https://www.youtube.com/channel/{channel.main.id}/",
            channel.general_data.country,
            channel.general_data.top_category,
            ",".join(channel.general_data.emails),
            channel.stats.subscribers,
            channel.stats.last_30day_subscribers,
            channel.stats.last_30day_views,
            channel.stats.views_per_video,
            channel.stats.sentiment,
            channel.stats.engage_rate,
            channel.stats.last_video_published_at.isoformat().replace("+00:00", "Z"),
            "",
            channel.ads_stats.video_view_rate,
            channel.ads_stats.ctr,
            channel.ads_stats.ctr_v,
            channel.ads_stats.average_cpv,
        ]
        expected_values_str = [str(value) for value in expected_values]
        self.assertEqual(
            data,
            expected_values_str
        )

    def test_missed_values(self):
        self.create_admin_user()
        channel = Channel(next(int_iterator))
        ChannelManager().upsert([channel])

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        id_index = 1
        values = [value for index, value in enumerate(data) if index != id_index]
        self.assertEqual(
            ["" for _ in range(len(values))],
            values
        )

    def test_brand_safety(self):
        self.create_admin_user()
        channel_id = str(next(int_iterator))
        channel = Channel(channel_id)
        ChannelManager().upsert([channel])
        brand_safety = ChannelBrandSafetyDoc(
            meta={'id': channel_id},
            overall_score=12.3
        )
        brand_safety.save()
        sleep(.3)

        with override_settings(BRAND_SAFETY_CHANNEL_INDEX=ChannelBrandSafetyDoc._index._name):
            response = self._request()

            csv_data = get_data_from_csv_response(response)
            headers = next(csv_data)
            data = next(csv_data)

        brand_safety_index = headers.index("brand_safety_score")
        self.assertEqual(
            str(brand_safety.overall_score),
            data[brand_safety_index]
        )


class ChannelBrandSafetyDoc(BaseDocument):
    """
    Temporary solution for testing brand safety.
    Remove this doc after implementing the Brand Safety feature in the dmp project
    """
    channel_id = Keyword()
    overall_score = Double()

    class Index:
        name = "channel_brand_safety"
        prefix = "channel_brand_safety_"

    class Meta:
        doc_type = "channel_brand_safety"


def get_data_from_csv_response(response):
    return csv.reader((row.decode("utf-8") for row in response.streaming_content))
