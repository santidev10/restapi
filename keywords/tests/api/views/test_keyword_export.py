import csv
from datetime import datetime

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.constants import Sections
from es_components.managers import KeywordManager
from es_components.models import Keyword
from es_components.tests.utils import ESTestCase
from keywords.api.names import KeywordPathName
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class KeywordListExportTestCase(ExtendedAPITestCase, ESTestCase):
    def _request(self, **query_params):
        url = reverse(
            KeywordPathName.KEYWORD_EXPORT,
            [Namespace.KEYWORD],
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
        user.add_custom_user_permission("keyword_list")

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_csv_response(self):
        test_datetime = datetime(2020, 3, 4, 5, 6, 7)
        self.create_admin_user()

        with patch_now(test_datetime):
            response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "text/csv")
        expected_filename = "Keywords export report {}.csv".format(test_datetime.strftime("%Y-%m-%d_%H-%m"))
        self.assertEqual(response["Content-Disposition"], "attachment; filename=\"{}\"".format(expected_filename))

    def test_headers(self):
        self.create_admin_user()

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "keyword",
            "search_volume",
            "average_cpc",
            "competition",
            "video_count",
            "views",
        ])

    def test_response(self):
        self.create_admin_user()
        keyword = Keyword(next(int_iterator))
        keyword.populate_stats(
            search_volume=1234,
            average_cpc=1.2,
            competition=2.3,
            video_count=234,
            views=3456,
        )
        manager = KeywordManager(sections=(Sections.STATS,))
        manager.upsert([keyword])

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        expected_values = [
            keyword.main.id,
            keyword.stats.search_volume,
            keyword.stats.average_cpc,
            keyword.stats.competition,
            keyword.stats.video_count,
            keyword.stats.views,
        ]
        expected_values_str = [str(value) for value in expected_values]
        self.assertEqual(
            data,
            expected_values_str
        )

    def test_missed_values(self):
        self.create_admin_user()
        keyword = Keyword(next(int_iterator))
        KeywordManager(sections=Sections.STATS).upsert([keyword])

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        id_index = 0
        values = [value for index, value in enumerate(data) if index != id_index]
        self.assertEqual(
            ["" for _ in range(len(values))],
            values
        )

    def test_filter_ids(self):
        self.create_admin_user()
        filter_count = 2
        keywords = [Keyword(next(int_iterator)) for _ in range(filter_count + 1)]
        KeywordManager(sections=Sections.STATS).upsert(keywords)
        keyword_ids = [str(keyword.main.id) for keyword in keywords]

        response = self._request(ids=",".join(keyword_ids[:filter_count]))

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    def test_filter_volume(self):
        self.create_admin_user()
        channels = [Keyword(next(int_iterator)) for _ in range(2)]
        channels[0].populate_stats(search_volume=1)
        channels[1].populate_stats(search_volume=3)
        KeywordManager(sections=Sections.STATS).upsert(channels)

        response = self._request(**{"stats.search_volume": "1,2"})
        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(1, len(data))


def get_data_from_csv_response(response):
    return csv.reader((row.decode("utf-8") for row in response.streaming_content))
