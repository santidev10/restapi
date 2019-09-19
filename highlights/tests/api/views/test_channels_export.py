from datetime import datetime
from unittest import mock

from django.urls import resolve
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN


from channel.api.serializers.channel_export import ChannelListExportSerializer
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.tests.utils import ESTestCase
from highlights.api.urls.names import HighlightsNames
from saas.urls.namespaces import Namespace
from utils.lang import ExtendedEnum
from utils.utittests.csv import get_data_from_csv_response
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.s3_mock import mock_s3

EXPORT_FILE_HASH = "test_channel_export_hash"


class HighlightChannelExportPermissionsApiViewTestCase(ExtendedAPITestCase, ESTestCase):

    @mock_s3
    def test_unauthorized(self):
        url = get_collect_file_url()
        response = self.client.post(url)

        self.assertEqual(
            HTTP_401_UNAUTHORIZED,
            response.status_code,
        )

    @mock_s3
    def test_forbidden(self):
        self.create_test_user()

        url = get_collect_file_url()
        response = self.client.post(url)

        self.assertEqual(
            HTTP_403_FORBIDDEN,
            response.status_code,
        )

    @mock_s3
    def test_has_permission(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_highlights")

        url = get_collect_file_url()
        response = self.client.post(url)

        self.assertEqual(
            HTTP_200_OK,
            response.status_code,
        )

    @mock_s3
    def test_admin(self):
        self.create_admin_user()

        url = get_collect_file_url()
        response = self.client.post(url)

        self.assertEqual(
            HTTP_200_OK,
            response.status_code,
        )


class HighlightChannelItemsApiViewTestCase(ExtendedAPITestCase, ESTestCase):

    def setUp(self):
        super(HighlightChannelItemsApiViewTestCase, self).setUp()
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_highlights")

    def _request_collect_file(self, **query_params):
        url = get_collect_file_url(**query_params)
        return self.client.post(url)

    def _request_export(self, export_name=EXPORT_FILE_HASH):
        url = get_export_url(export_name)
        return self.client.get(url)

    @mock_s3
    @mock.patch("highlights.api.views.channels_export.HighlightChannelsExportApiView.generate_report_hash",
                    return_value=EXPORT_FILE_HASH)
    def test_success_allowed_user(self, *args):
        user = self.create_test_user()
        user.add_custom_user_permission("view_highlights")
        self._request_collect_file()

        user.remove_custom_user_permission("view_highlights")
        response = self._request_export()
        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
    @mock.patch("highlights.api.views.channels_export.HighlightChannelsExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_success_csv_response(self, *args):
        test_datetime = datetime(2020, 3, 4, 5, 6, 7)
        self.create_admin_user()

        with patch_now(test_datetime):
            self._request_collect_file()
            response = self._request_export()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "application/CSV")

    @mock_s3
    @mock.patch("highlights.api.views.channels_export.HighlightChannelsExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_sorting_30day_views(self, *args):
        views = [1, 3, 2]
        channels = [Channel(next(int_iterator)) for _ in range(len(views))]
        for channel, item_views in zip(channels, views):
            channel.populate_stats(last_30day_views=item_views, observed_videos_count=10)
        ChannelManager(sections=[Sections.GENERAL_DATA, Sections.STATS]).upsert(channels)

        self._request_collect_file(sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)
        response = self._request_export()

        data = get_data_from_csv_response(response)
        headers = next(data)

        views_index = headers.index("thirty_days_views")

        response_views = [int(row[views_index]) for row in data]

        self.assertEqual(
            list(sorted(views, reverse=True)),
            response_views
        )

    @mock_s3
    @mock.patch("highlights.api.views.channels_export.HighlightChannelsExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_limit_100(self, *args):
        highlights_limit = 100
        channels = [Channel(next(int_iterator)) for _ in range(highlights_limit + 1)]
        for channel in channels:
            channel.populate_stats(observed_videos_count=10)

        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert(channels)

        self._request_collect_file()
        response = self._request_export()

        data = list(get_data_from_csv_response(response))[1:]
        self.assertEqual(
            highlights_limit,
            len(data)
        )

    @mock_s3
    @mock.patch("highlights.api.views.channels_export.HighlightChannelsExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_export_by_ids(self, *args):
        channels = [Channel(next(int_iterator)) for _ in range(2)]
        for channel in channels:
            channel.populate_stats(observed_videos_count=10)
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert(channels)

        self._request_collect_file(**{"main.id": channels[0].main.id})
        response = self._request_export()

        data = list(get_data_from_csv_response(response))[1:]
        self.assertEqual(
            1,
            len(data)
        )

    @mock_s3
    @mock.patch("highlights.api.views.channels_export.HighlightChannelsExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_export_by_ids_deprecated(self, *args):
        channels = [Channel(next(int_iterator)) for _ in range(2)]
        for channel in channels:
            channel.populate_stats(observed_videos_count=10)
        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert(channels)

        self._request_collect_file(**{"ids": channels[0].main.id})
        response = self._request_export()

        data = list(get_data_from_csv_response(response))[1:]
        self.assertEqual(
            1,
            len(data)
        )


class AllowedSorts(ExtendedEnum):
    VIEWS_30_DAYS_DESC = "stats.last_30day_views:desc"


def get_collect_file_url(**kwargs):
    return reverse(HighlightsNames.CHANNELS_PREPARE_EXPORT, [Namespace.HIGHLIGHTS], query_params=kwargs or None)


def get_export_url(export_name):
    return reverse(HighlightsNames.CHANNELS_EXPORT, [Namespace.HIGHLIGHTS], args=(export_name,),)
