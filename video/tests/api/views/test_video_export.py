from datetime import datetime
from math import floor
from unittest import mock

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
from utils.unittests.csv import get_data_from_csv_response
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.s3_mock import mock_s3
from video.api.urls.names import Name

import brand_safety.constants as constants

EXPECT_MESSSAGE = "File is in queue for preparing. After it is finished exporting, " \
                  "you will receive message via email."

EXPORT_FILE_HASH = "7386e05b6106efe72c2ac0b361552556"


class VideoListPrepareExportTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, **query_params):
        return reverse(
            Name.VIDEO_PREPARE_EXPORT,
            [Namespace.VIDEO],
            query_params=query_params,
        )

    def _request(self, **query_params):
        url = self._get_url(**query_params)
        return self.client.post(url)

    @mock_s3
    def test_not_auth(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    @mock_s3
    def test_no_permissions(self):
        self.create_test_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @mock_s3
    def test_success_admin(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
    def test_success_allowed_user(self):
        user = self.create_test_user()
        user.add_custom_user_permission("research_exports")

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
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

        response = self._request(emails="test@test.test")

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(EXPECT_MESSSAGE, response.data.get("message"))

    @mock_s3
    def test_filter(self):
        self.create_admin_user()
        filter_count = 2
        videos = [Video(next(int_iterator)) for _ in range(filter_count + 1)]
        VideoManager(sections=Sections.GENERAL_DATA).upsert(videos)
        video_ids = [str(video.main.id) for video in videos]

        response = self._request(**{"main.id": ",".join(video_ids[:filter_count])})

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(EXPECT_MESSSAGE, response.data.get("message"))

    @mock_s3
    def test_success_request_send_twice(self):
        self.create_admin_user()

        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)

        response2 = self._request()
        self.assertIsNotNone(response2.data.get("export_url"))


class VideoListExportTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, export_name):
        return reverse(
            Name.VIDEO_EXPORT,
            [Namespace.VIDEO],
            args=(export_name,),
        )

    def _request(self, export_name=EXPORT_FILE_HASH):
        url = self._get_url(export_name)
        return self.client.get(url)

    def _request_collect_file(self, **query_params):
        collect_file_url = reverse(
            Name.VIDEO_PREPARE_EXPORT,
            [Namespace.VIDEO],
            query_params=query_params,
        )
        self.client.post(collect_file_url)

    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_success_allowed_user(self, *args):
        user = self.create_test_user()
        user.add_custom_user_permission("research_exports")
        self._request_collect_file()

        user.remove_custom_user_permission("video_list")
        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_missed_values(self, *args):
        self.create_admin_user()
        video = Video(next(int_iterator))
        VideoManager(sections=Sections.GENERAL_DATA).upsert([video])

        self._request_collect_file()
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        id_index = 1
        values = [value for index, value in enumerate(data) if index != id_index]
        expected_values = ["" for _ in range(len(values))]
        expected_values[1] = "[]"
        self.assertEqual(
            expected_values,
            values
        )

    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_ids(self, *args):
        self.create_admin_user()
        filter_count = 2
        videos = [Video(next(int_iterator)) for _ in range(filter_count + 1)]
        VideoManager(sections=Sections.GENERAL_DATA).upsert(videos)
        video_ids = [str(video.main.id) for video in videos]

        self._request_collect_file(**{"main.id": ",".join(video_ids[:filter_count])})
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    @mock.patch("utils.es_components_api_utils.ExportDataGenerator.export_limit", 2)
    def test_export_limitation(self, *args):
        self.create_admin_user()
        filter_count = 2
        videos = [Video(next(int_iterator)) for _ in range(filter_count + 5)]
        VideoManager(sections=Sections.GENERAL_DATA).upsert(videos)

        self._request_collect_file()
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )


    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_ids_deprecated(self, *args):
        self.create_admin_user()
        filter_count = 2
        videos = [Video(next(int_iterator)) for _ in range(filter_count + 1)]
        VideoManager(sections=Sections.GENERAL_DATA).upsert(videos)
        video_ids = [str(video.main.id) for video in videos]

        self._request_collect_file(ids=",".join(video_ids[:filter_count]))

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_verified(self, *args):
        self.create_admin_user()
        videos = [Video(next(int_iterator)) for _ in range(2)]
        VideoManager(sections=Sections.GENERAL_DATA).upsert([videos[0]])
        VideoManager(sections=(Sections.GENERAL_DATA, Sections.ANALYTICS)).upsert([videos[1]])

        self._request_collect_file(analytics="true")

        response = self._request()
        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(1, len(data))

    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_headers(self, *args):
        self.create_admin_user()

        self._request_collect_file()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "application/CSV")

        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "title",
            "url",
            "iab_categories",
            "views",
            "monthly_views",
            "weekly_views",
            "daily_views",
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

    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_brand_safety(self, *args):
        self.create_admin_user()

        videos = [Video(next(int_iterator)) for _ in range(2)]

        for video in videos:
            video.populate_brand_safety(overall_score=50)
        VideoManager(sections=Sections.GENERAL_DATA).upsert([videos[0]])
        VideoManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY)).upsert([videos[1]])

        self._request_collect_file(brand_safety=constants.HIGH_RISK)
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(1, len(data))

    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_brand_safety_not_allowed(self, *args):
        user = self.create_test_user()
        user.add_custom_user_permission("research_exports")

        videos = [Video(next(int_iterator)) for _ in range(2)]

        for video in videos:
            video.populate_brand_safety(overall_score=50)
        VideoManager(sections=Sections.GENERAL_DATA).upsert([videos[0]])
        VideoManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY)).upsert([videos[1]])

        self._request_collect_file(brand_safety=constants.HIGH_RISK)
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(2, len(data))

    @mock_s3
    @mock.patch("video.api.views.video_export.VideoListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_brand_safety_score_mapped(self, *args):
        user = self.create_test_user()
        user.add_custom_user_permission("research_exports")

        manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY))
        videos = [Video(next(int_iterator)) for _ in range(2)]

        videos[0].populate_brand_safety(overall_score=54)
        videos[1].populate_brand_safety(overall_score=87)

        manager.upsert(videos)

        self._request_collect_file(brand_safety=constants.HIGH_RISK)
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)
        rows = sorted(data[1:], key=lambda x: x[11])
        self.assertEqual(5, int(rows[0][11]))
        self.assertEqual(8, int(rows[1][11]))
