import json
from datetime import datetime
from unittest import mock

import pytz
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

import brand_safety.constants as constants
from channel.api.urls.names import ChannelPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.models.channel import ChannelSectionBrandSafety
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.brand_safety import map_brand_safety_score
from utils.unittests.csv import get_data_from_csv_response
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.s3_mock import mock_s3
from utils.unittests.test_case import ExtendedAPITestCase

EXPORT_FILE_HASH = "7386e05b6106efe72c2ac0b361552556"


class ChannelListPrepareExportTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, **query_params):
        return reverse(
            ChannelPathName.CHANNEL_LIST_PREPARE_EXPORT,
            [Namespace.CHANNEL],
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
    def test_success_request_send_twice(self):
        self.create_admin_user()

        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)

        response2 = self._request()
        self.assertIsNotNone(response2.data.get("export_url"))

    @mock_s3
    def test_success_allowed_user(self):
        user = self.create_test_user()
        user.add_custom_user_permission("research_exports")

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)


class ChannelListExportTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, export_name):
        return reverse(
            ChannelPathName.CHANNEL_LIST_EXPORT,
            [Namespace.CHANNEL],
            args=(export_name,),
        )

    def _get_collect_file_url(self, **query_params):
        return reverse(
            ChannelPathName.CHANNEL_LIST_PREPARE_EXPORT,
            [Namespace.CHANNEL],
            query_params=query_params,
        )

    def _request(self, export_name=EXPORT_FILE_HASH):
        url = self._get_url(export_name)
        return self.client.get(url)

    def _request_collect_file(self, **query_params):
        collect_file_url = self._get_collect_file_url(**query_params)
        self.client.post(collect_file_url)

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_success_allowed_user(self, *args):
        user = self.create_test_user()
        user.add_custom_user_permission("research_exports")
        self._request_collect_file()

        user.remove_custom_user_permission("channel_list")
        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_headers(self, *args):
        self.create_admin_user()

        self._request_collect_file()
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "title",
            "url",
            "country",
            "language",
            "iab_categories",
            "subscribers",
            "thirty_days_subscribers",
            "views",
            "monthly_views",
            "weekly_views",
            "daily_views",
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

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_response(self, *args):
        self.create_admin_user()
        channel = Channel(next(int_iterator))
        channel.populate_general_data(
            title="Test channel title",
            country_code="RU",
            top_lang_code="ru",
            iab_categories=["Top category",],
            emails=["example1@mail.com", "example2@email.com"],
        )
        channel.populate_stats(
            subscribers=123,
            last_30day_subscribers=12,
            last_30day_views=321,
            last_7day_views=101,
            last_day_views=20,
            views=3000,
            views_per_video=123.4,
            sentiment=0.23,
            total_videos_count=10,
            engage_rate=0.34,
            last_video_published_at=datetime(2018, 2, 3, 4, 5, 6, tzinfo=pytz.utc),
        )
        channel.populate_ads_stats(
            video_view_rate=12.3,
            ctr=12.3,
            ctr_v=23.4,
            average_cpv=34.5,
        )
        channel.brand_safety = ChannelSectionBrandSafety(overall_score=12)
        manager = ChannelManager(
            sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS, Sections.BRAND_SAFETY))
        manager.upsert([channel])

        self._request_collect_file()
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        expected_values = [
            channel.general_data.title,
            f"https://www.youtube.com/channel/{channel.main.id}",
            "RU",
            "Russian",
            ",".join(channel.general_data.iab_categories),
            channel.stats.subscribers,
            channel.stats.last_30day_subscribers,
            channel.stats.views,
            channel.stats.last_30day_views,
            channel.stats.last_7day_views,
            channel.stats.last_day_views,
            channel.stats.views_per_video,
            channel.stats.sentiment,
            channel.stats.engage_rate,
            channel.stats.last_video_published_at.isoformat().replace("+00:00", "Z"),
            map_brand_safety_score(channel.brand_safety.overall_score),
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

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_missed_values(self, *args):
        self.create_admin_user()
        channel = Channel(next(int_iterator))
        channel.populate_stats(total_videos_count=10)
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert([channel])

        self._request_collect_file()
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        id_index = 1
        values = [value for index, value in enumerate(data) if index != id_index]
        expected_values = ["" for _ in range(len(values))]
        expected_values[2] = ""
        self.assertEqual(
            expected_values,
            values
        )

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_ids(self, *args):
        self.create_admin_user()
        filter_count = 2
        channels = [Channel(next(int_iterator)) for _ in range(filter_count + 1)]
        for channel in channels:
            channel.populate_stats(total_videos_count=10)
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert(channels)
        channel_ids = [str(channel.main.id) for channel in channels]

        self._request_collect_file(ids=",".join(channel_ids[:filter_count]))
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    @mock.patch("utils.es_components_api_utils.ExportDataGenerator.export_limit", 2)
    def test_export_limitation(self, *args):
        self.create_admin_user()
        filter_count = 2
        channels = [Channel(next(int_iterator)) for _ in range(filter_count + 5)]
        for channel in channels:
            channel.populate_stats(total_videos_count=10)
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert(channels)

        self._request_collect_file()
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_ids_in_payload(self, *args):
        self.create_admin_user()
        filter_count = 2
        channels = [Channel(next(int_iterator)) for _ in range(filter_count + 1)]
        for channel in channels:
            channel.populate_stats(total_videos_count=10)
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert(channels)
        channel_ids = [str(channel.main.id) for channel in channels]

        payload = {"main.id": channel_ids[:filter_count]}
        self.client.post(self._get_collect_file_url(), data=json.dumps(payload), content_type="application/json")
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_brand_safety(self, *args):
        self.create_admin_user()
        channels = [Channel(next(int_iterator)) for _ in range(2)]
        for channel in channels:
            channel.populate_stats(total_videos_count=10)
            channel.populate_brand_safety(overall_score=50)
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert([channels[0]])
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS)).upsert([channels[1]])

        self._request_collect_file(brand_safety=constants.LOW_RISK)
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(1, len(data))

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_brand_safety_not_allowed(self, *args):
        user = self.create_test_user()
        user.add_custom_user_permission("research_exports")

        channels = [Channel(next(int_iterator)) for _ in range(2)]
        for channel in channels:
            channel.populate_stats(total_videos_count=10)
            channel.populate_brand_safety(overall_score=50)
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert([channels[0]])
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS)).upsert([channels[1]])

        self._request_collect_file(brand_safety=constants.LOW_RISK)
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(2, len(data))

    @mock_s3
    @mock.patch("channel.api.views.channel_export.ChannelListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_brand_safety_score_mapped(self, *args):
        self.create_admin_user()
        channels = [Channel(next(int_iterator)) for _ in range(2)]
        channels[0].populate_brand_safety(overall_score=49)
        channels[0].populate_stats(total_videos_count=100)
        channels[1].populate_brand_safety(overall_score=62)
        channels[1].populate_stats(total_videos_count=100)
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS)).upsert(channels)

        self._request_collect_file(brand_safety=constants.LOW_RISK)
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)
        rows = sorted(data[1:], key=lambda x: x[15])
        self.assertEqual(4, int(rows[0][15]))
        self.assertEqual(6, int(rows[1][15]))
