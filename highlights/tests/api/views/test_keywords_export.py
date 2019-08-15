from datetime import datetime

from django.urls import resolve
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.constants import Sections
from es_components.managers import KeywordManager
from es_components.models import Keyword
from es_components.tests.utils import ESTestCase
from highlights.api.urls.names import HighlightsNames
from keywords.api.views.keyword_export import KeywordCSVRendered
from keywords.api.views.keyword_export import KeywordListExportSerializer
from saas.urls.namespaces import Namespace
from utils.utittests.csv import get_data_from_csv_response
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class HighlightKeywordExportPermissionsApiViewTestCase(ExtendedAPITestCase, ESTestCase):

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


class HighlightKeywordExportApiViewTestCase(ExtendedAPITestCase, ESTestCase):

    def setUp(self):
        super(HighlightKeywordExportApiViewTestCase, self).setUp()
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
        expected_filename = "Keywords export report {}.csv".format(test_datetime.strftime("%Y-%m-%d_%H-%m"))
        self.assertEqual(response["Content-Disposition"], "attachment; filename=\"{}\"".format(expected_filename))

    def test_view_declaration(self):
        """
        This test checks view declaration. The functional part is tested in keyword/tests/api/test_keyword_export.py
        """
        resolver = resolve(get_url())
        view_cls = resolver.func.cls
        self.assertEqual((KeywordCSVRendered,), view_cls.renderer_classes)
        self.assertEqual(KeywordListExportSerializer, view_cls.serializer_class)

    def test_limit_100(self):
        highlights_limit = 100
        keywords = [Keyword(next(int_iterator)) for _ in range(highlights_limit + 1)]
        KeywordManager(sections=(Sections.STATS)).upsert(keywords)

        response = self._request()

        data = list(get_data_from_csv_response(response))[1:]
        self.assertEqual(
            highlights_limit,
            len(data)
        )


def get_url(**kwargs):
    return reverse(HighlightsNames.KEYWORDS_EXPORT, [Namespace.HIGHLIGHTS],
                   query_params=kwargs or None)
