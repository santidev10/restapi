import csv
from datetime import datetime
from time import sleep
from unittest.mock import patch

import pytz
from django.test import override_settings
from elasticsearch_dsl import Double
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from es_components.models.base import BaseDocument
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.elasticsearch import ElasticSearchConnector
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase
from video.api.urls.names import Name


class VideoListExportTestCase(ExtendedAPITestCase, ESTestCase):
    def _request(self, **query_params):
        url = reverse(
            Name.VIDEO_EXPORT,
            [Namespace.VIDEO],
            query_params=query_params,
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
        user.add_custom_user_permission("video_list")

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_csv_response(self):
        test_datetime = datetime(2020, 3, 4, 5, 6, 7)
        self.create_admin_user()

        with patch_now(test_datetime):
            response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "text/csv")
        expected_filename = "Videos export report {}.csv".format(test_datetime.strftime("%Y-%m-%d_%H-%m"))
        self.assertEqual(response["Content-Disposition"], "attachment; filename=\"{}\"".format(expected_filename))

    def test_headers(self):
        self.create_admin_user()

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "title",
            "url",
            "views",
            "likes",
            "dislikes",
            "comments",
            "youtube_published_at",
            "brand_safety_score",
            "video_view_rate",
            "ctr",
            "ctr_v",
            "average_cpv",
        ])

    def test_response(self):
        self.create_admin_user()
        video = Video(next(int_iterator))
        video.populate_general_data(
            title="Test video title",
            youtube_published_at=datetime(2018, 2, 3, 4, 5, 6, tzinfo=pytz.utc),
        )
        video.populate_stats(
            views=1234,
            likes=123,
            dislikes=234,
            comments=345,
        )
        video.populate_ads_stats(
            video_view_rate=3.23,
            ctr=2.3,
            ctr_v=3.4,
            average_cpv=4.5,
        )
        manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS))
        manager.upsert([video])

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        expected_values = [
            video.general_data.title,
            f"https://www.youtube.com/watch?v={video.main.id}",
            video.stats.views,
            video.stats.likes,
            video.stats.dislikes,
            video.stats.comments,
            video.general_data.youtube_published_at.isoformat().replace("+00:00", "Z"),
            "",
            video.ads_stats.video_view_rate,
            video.ads_stats.ctr,
            video.ads_stats.ctr_v,
            video.ads_stats.average_cpv,
        ]
        expected_values_str = [str(value) for value in expected_values]
        self.assertEqual(
            data,
            expected_values_str
        )

    def test_missed_values(self):
        self.create_admin_user()
        video = Video(next(int_iterator))
        VideoManager().upsert([video])

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
        video_id = str(next(int_iterator))
        video = Video(video_id)
        VideoManager().upsert([video])
        brand_safety = VideoBrandSafetyDoc(
            meta={'id': video_id},
            overall_score=12.3
        )
        brand_safety.save()
        sleep(.5)

        with override_settings(BRAND_SAFETY_VIDEO_INDEX=VideoBrandSafetyDoc._index._name):
            response = self._request()

            csv_data = get_data_from_csv_response(response)
            headers = next(csv_data)
            data = next(csv_data)

        brand_safety_index = headers.index("brand_safety_score")
        self.assertEqual(
            str(brand_safety.overall_score),
            data[brand_safety_index]
        )

    def test_request_brand_safety_by_batches(self):
        self.create_admin_user()
        videos = [Video(next(int_iterator)) for _ in range(2)]
        VideoManager().upsert(videos)

        with patch.object(ElasticSearchConnector, "search_by_id", return_value={}) as es_mock:
            response = self._request()

            csv_data = get_data_from_csv_response(response)
            list(csv_data)

            es_mock.assert_called_once()

    def test_filter(self):
        self.create_admin_user()
        filter_count = 2
        videos = [Video(next(int_iterator)) for _ in range(filter_count + 1)]
        VideoManager().upsert(videos)
        video_ids = [str(video.main.id) for video in videos]

        response = self._request(ids=",".join(video_ids[:filter_count]))

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )


class VideoBrandSafetyDoc(BaseDocument):
    """
    Temporary solution for testing brand safety.
    Remove this doc after implementing the Brand Safety feature in the dmp project
    """
    overall_score = Double()

    class Index:
        name = "video_brand_safety"
        prefix = "video_brand_safety_"

    class Meta:
        doc_type = "video_brand_safety"


def get_data_from_csv_response(response):
    return csv.reader((row.decode("utf-8") for row in response.streaming_content))
