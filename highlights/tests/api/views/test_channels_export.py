from datetime import datetime

from django.urls import resolve
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from channel.api.views.channel_export import ChannelCSVRendered
from channel.api.views.channel_export import ChannelListExportSerializer
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


class HighlightChannelExportPermissionsApiViewTestCase(ExtendedAPITestCase, ESTestCase):

    def test_unauthorized(self):
        url = get_url()
        response = self.client.get(url)

        self.assertEqual(
            HTTP_401_UNAUTHORIZED,
            response.status_code,
        )

    def test_forbidden(self):
        self.create_test_user()

        url = get_url()
        response = self.client.get(url)

        self.assertEqual(
            HTTP_403_FORBIDDEN,
            response.status_code,
        )

    def test_has_permission(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_highlights")

        url = get_url()
        response = self.client.get(url)

        self.assertEqual(
            HTTP_200_OK,
            response.status_code,
        )

    def test_admin(self):
        self.create_admin_user()

        url = get_url()
        response = self.client.get(url)

        self.assertEqual(
            HTTP_200_OK,
            response.status_code,
        )


class HighlightChannelItemsApiViewTestCase(ExtendedAPITestCase, ESTestCase):

    def setUp(self):
        super(HighlightChannelItemsApiViewTestCase, self).setUp()
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_highlights")

    def _request(self, **query_params):
        url = get_url(**query_params)
        return self.client.get(url)

    def test_success_csv_response(self):
        test_datetime = datetime(2020, 3, 4, 5, 6, 7)
        self.create_admin_user()

        with patch_now(test_datetime):
            response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "text/csv")
        expected_filename = "Channels export report {}.csv".format(test_datetime.strftime("%Y-%m-%d_%H-%m"))
        self.assertEqual(response["Content-Disposition"], "attachment; filename=\"{}\"".format(expected_filename))

    def test_view_declaration(self):
        """
        This test checks view declaration. The functional part is tested in channel/tests/api/test_channel_export.py
        """
        resolver = resolve(get_url())
        view_cls = resolver.func.cls
        self.assertEqual((ChannelCSVRendered,), view_cls.renderer_classes)
        self.assertEqual(ChannelListExportSerializer, view_cls.serializer_class)

    def test_sorting_30day_views(self):
        views = [1, 3, 2]
        channels = [Channel(next(int_iterator)) for _ in range(len(views))]
        for channel, item_views in zip(channels, views):
            channel.populate_stats(last_30day_views=item_views, observed_videos_count=10)
        ChannelManager(sections=[Sections.GENERAL_DATA, Sections.STATS]).upsert(channels)

        response = self._request(sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)

        data = get_data_from_csv_response(response)
        headers = next(data)

        views_index = headers.index("thirty_days_views")

        response_views = [int(row[views_index]) for row in data]

        self.assertEqual(
            list(sorted(views, reverse=True)),
            response_views
        )

    def test_limit_100(self):
        highlights_limit = 100
        channels = [Channel(next(int_iterator)) for _ in range(highlights_limit + 1)]
        for channel in channels:
            channel.populate_stats(observed_videos_count=10)

        ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS)).upsert(channels)

        response = self._request()

        data = list(get_data_from_csv_response(response))[1:]
        self.assertEqual(
            highlights_limit,
            len(data)
        )


class AllowedSorts(ExtendedEnum):
    VIEWS_30_DAYS_DESC = "stats.last_30day_views:desc"


def get_url(**kwargs):
    return reverse(HighlightsNames.CHANNELS_EXPORT, [Namespace.HIGHLIGHTS],
                   query_params=kwargs or None)
