from datetime import datetime
from unittest import mock

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.constants import Sections
from es_components.managers import KeywordManager
from es_components.models import Keyword
from es_components.tests.utils import ESTestCase
from keywords.api.names import KeywordPathName
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.csv import get_data_from_csv_response
from utils.unittests.int_iterator import int_iterator
from utils.unittests.patch_now import patch_now
from utils.unittests.reverse import reverse
from utils.unittests.s3_mock import mock_s3
from utils.unittests.test_case import ExtendedAPITestCase

EXPORT_FILE_HASH = "7386e05b6106efe72c2ac0b361552556"


class KeywordListPrepareExportTestCase(ExtendedAPITestCase, ESTestCase):

    def _get_url(self, **query_params):
        return reverse(
            KeywordPathName.KEYWORD_PREPARE_EXPORT,
            [Namespace.KEYWORD],
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
        self.create_test_user(perms={StaticPermissions.RESEARCH__EXPORT: False})

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @mock_s3
    def test_success_admin(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
    def test_success_allowed_user(self):
        self.create_test_user(perms={
            StaticPermissions.RESEARCH__EXPORT: True,
        })
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
    def test_success_request_send_twice(self):
        self.create_admin_user()

        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)

        response2 = self._request()
        self.assertIsNotNone(response2.data.get("export_url"))


class KeywordListExportTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, export_name):
        return reverse(
            KeywordPathName.KEYWORD_EXPORT,
            [Namespace.KEYWORD],
            args=(export_name,),
        )

    def _request(self, export_name=EXPORT_FILE_HASH):
        url = self._get_url(export_name)
        return self.client.get(url)

    def _request_collect_file(self, **query_params):
        collect_file_url = reverse(
            KeywordPathName.KEYWORD_PREPARE_EXPORT,
            [Namespace.KEYWORD],
            query_params=query_params,
        )
        self.client.post(collect_file_url)

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_success_allowed_user(self, *args):
        self.create_test_user(perms={
            StaticPermissions.RESEARCH__EXPORT: True,
        })
        self._request_collect_file()

        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_success_csv_response(self, *args):
        test_datetime = datetime(2020, 3, 4, 5, 6, 7)
        self.create_admin_user()

        with patch_now(test_datetime):
            self._request_collect_file()
            response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "application/CSV")

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_headers(self, *args):
        self.create_admin_user()

        self._request_collect_file()
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

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_response(self, *args):
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

        self._request_collect_file()
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

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_missed_values(self, *args):
        self.create_admin_user()
        keyword = Keyword(next(int_iterator))
        KeywordManager(sections=Sections.STATS).upsert([keyword])

        self._request_collect_file()
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1]
        id_index = 0
        values = [value for index, value in enumerate(data) if index != id_index]
        self.assertEqual(
            ["" for _ in range(len(values))],
            values
        )

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_ids(self, *args):
        self.create_admin_user()
        filter_count = 1
        keywords = [Keyword(next(int_iterator)) for _ in range(filter_count + 1)]
        KeywordManager(sections=Sections.STATS).upsert(keywords)
        keyword_ids = [str(keyword.main.id) for keyword in keywords]

        self._request_collect_file(**{"main.id": ",".join(keyword_ids[:filter_count])})
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    @mock.patch("utils.es_components_api_utils.ExportDataGenerator.export_limit", 2)
    def test_export_limitation(self, *args):
        self.create_admin_user()
        filter_count = 2
        keywords = [Keyword(next(int_iterator)) for _ in range(filter_count + 5)]
        KeywordManager(sections=Sections.STATS).upsert(keywords)

        self._request_collect_file()
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_ids_deprecated(self, *args):
        self.create_admin_user()
        filter_count = 2
        keywords = [Keyword(next(int_iterator)) for _ in range(filter_count + 1)]
        KeywordManager(sections=Sections.STATS).upsert(keywords)
        keyword_ids = [str(keyword.main.id) for keyword in keywords]

        self._request_collect_file(ids=",".join(keyword_ids[:filter_count]))
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(
            filter_count,
            len(data)
        )

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_volume(self, *args):
        self.create_admin_user()
        keywords = [Keyword(next(int_iterator)) for _ in range(2)]
        keywords[0].populate_stats(search_volume=1)
        keywords[1].populate_stats(search_volume=3)
        KeywordManager(sections=Sections.STATS).upsert(keywords)

        self._request_collect_file(**{"stats.search_volume": "1,2"})
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(1, len(data))

    @mock_s3
    @mock.patch("keywords.api.views.keyword_export.KeywordListExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_filter_viral(self, *args):
        self.create_admin_user()
        keywords = [Keyword(next(int_iterator)) for _ in range(2)]
        keywords[0].populate_stats(is_viral=True)
        KeywordManager(sections=Sections.STATS).upsert(keywords)

        self._request_collect_file(**{"stats.is_viral": "Viral"})
        response = self._request()

        csv_data = get_data_from_csv_response(response)
        data = list(csv_data)[1:]

        self.assertEqual(1, len(data))
