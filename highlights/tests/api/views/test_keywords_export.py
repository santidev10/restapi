from datetime import datetime
from unittest import mock

from django.urls import resolve
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.constants import Sections
from es_components.managers import KeywordManager
from es_components.models import Keyword
from es_components.tests.utils import ESTestCase
from highlights.api.urls.names import HighlightsNames
from saas.urls.namespaces import Namespace
from utils.unittests.csv import get_data_from_csv_response
from utils.unittests.int_iterator import int_iterator
from utils.unittests.patch_now import patch_now
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.s3_mock import mock_s3


EXPORT_FILE_HASH = "7386e05b6106efe72c2ac0b361552556"


class HighlightKeywordExportPermissionsApiViewTestCase(ExtendedAPITestCase, ESTestCase):

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

    @mock_s3
    def test_success_request_send_twice(self):
        self.create_admin_user()

        url = get_collect_file_url()

        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        response2 = self.client.post(url)
        self.assertIsNotNone(response2.data.get("export_url"))


class HighlightKeywordExportApiViewTestCase(ExtendedAPITestCase, ESTestCase):

    def setUp(self):
        super(HighlightKeywordExportApiViewTestCase, self).setUp()
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_highlights")

    def _request_collect_file(self, **query_params):
        url = get_collect_file_url(**query_params)
        return self.client.post(url)

    def _request_export(self, export_name=EXPORT_FILE_HASH):
        url = get_export_url(export_name)
        return self.client.get(url)

    @mock_s3
    @mock.patch("highlights.api.views.keywords_export.HighlightKeywordsExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_success_allowed_user(self, *args):
        user = self.create_test_user()
        user.add_custom_user_permission("view_highlights")
        self._request_collect_file()

        user.remove_custom_user_permission("view_highlights")
        response = self._request_export()
        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
    @mock.patch("highlights.api.views.keywords_export.HighlightKeywordsExportApiView.generate_report_hash",
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
    @mock.patch("highlights.api.views.keywords_export.HighlightKeywordsExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_limit_100(self, *args):
        highlights_limit = 100
        keywords = [Keyword(next(int_iterator)) for _ in range(highlights_limit + 1)]
        KeywordManager(sections=(Sections.STATS,)).upsert(keywords)

        self._request_collect_file()
        response = self._request_export()

        data = list(get_data_from_csv_response(response))[1:]
        self.assertEqual(
            highlights_limit,
            len(data)
        )

    @mock_s3
    @mock.patch("highlights.api.views.keywords_export.HighlightKeywordsExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_export_by_ids(self, *args):
        keywords = [Keyword(next(int_iterator)) for _ in range(2)]
        KeywordManager(sections=(Sections.STATS,)).upsert(keywords)

        self._request_collect_file(**{"main.id": keywords[0].main.id})
        response = self._request_export()

        data = list(get_data_from_csv_response(response))[1:]
        self.assertEqual(
            1,
            len(data)
        )

    @mock_s3
    @mock.patch("highlights.api.views.keywords_export.HighlightKeywordsExportApiView.generate_report_hash",
                return_value=EXPORT_FILE_HASH)
    def test_export_by_ids_deprecated(self, *args):
        keywords = [Keyword(next(int_iterator)) for _ in range(2)]
        KeywordManager(sections=(Sections.STATS,)).upsert(keywords)

        self._request_collect_file(**{"ids": keywords[0].main.id})
        response = self._request_export()

        data = list(get_data_from_csv_response(response))[1:]
        self.assertEqual(
            1,
            len(data)
        )


def get_collect_file_url(**kwargs):
    return reverse(HighlightsNames.KEYWORDS_PREPARE_EXPORT, [Namespace.HIGHLIGHTS], query_params=kwargs or None)


def get_export_url(export_name):
    return reverse(HighlightsNames.KEYWORDS_EXPORT, [Namespace.HIGHLIGHTS], args=(export_name,),)

