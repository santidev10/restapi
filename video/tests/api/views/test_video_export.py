import json
from datetime import datetime

import pytz
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from es_components.models.video import VideoSectionBrandSafety
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.utittests.csv import get_data_from_csv_response
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase
from video.api.urls.names import Name


class VideoListExportTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, **query_params):
        return reverse(
            Name.VIDEO_EXPORT,
            [Namespace.VIDEO],
            query_params=query_params,
        )

    def _request(self, **query_params):
        url = self._get_url(**query_params)
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
        video.brand_safety = VideoSectionBrandSafety(
            overall_score=12,
        )
        manager = VideoManager(
            sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS, Sections.BRAND_SAFETY))
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
            video.brand_safety.overall_score,
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
        VideoManager(sections=Sections.GENERAL_DATA).upsert([video])

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        id_index = 1
        values = [value for index, value in enumerate(data) if index != id_index]
        self.assertEqual(
            ["" for _ in range(len(values))],
            values
        )

    def test_filter_ids(self):
        self.create_admin_user()
        filter_count = 2
        videos = [Video(next(int_iterator)) for _ in range(filter_count + 1)]
        VideoManager(sections=Sections.GENERAL_DATA).upsert(videos)
        video_ids = [str(video.main.id) for video in videos]

        response = self._request(**{"main.id": ",".join(video_ids[:filter_count])})

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    def test_filter_ids_deprecated(self):
        self.create_admin_user()
        filter_count = 2
        videos = [Video(next(int_iterator)) for _ in range(filter_count + 1)]
        VideoManager(sections=Sections.GENERAL_DATA).upsert(videos)
        video_ids = [str(video.main.id) for video in videos]

        response = self._request(ids=",".join(video_ids[:filter_count]))

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    def test_filter_verified(self):
        self.create_admin_user()
        channels = [Video(next(int_iterator)) for _ in range(2)]
        VideoManager(sections=Sections.GENERAL_DATA).upsert([channels[0]])
        VideoManager(sections=(Sections.GENERAL_DATA, Sections.ANALYTICS)).upsert([channels[1]])

        response = self._request(analytics="true")
        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(1, len(data))

    def test_filter_ids_post_body(self):
        self.create_admin_user()
        filter_count = 2
        videos = [Video(next(int_iterator)) for _ in range(filter_count + 1)]
        VideoManager(sections=Sections.GENERAL_DATA).upsert(videos)
        video_ids = [str(video.main.id) for video in videos]

        url = self._get_url()
        payload = {
            "main.id": video_ids[:filter_count]
        }
        response = self.client.post(url, data=json.dumps(payload), content_type="application/json")
        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(filter_count, len(data))
