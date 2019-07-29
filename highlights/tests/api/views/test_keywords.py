from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.constants import Sections
from es_components.managers import KeywordManager
from es_components.models import Keyword
from es_components.tests.utils import ESTestCase
from highlights.api.urls.names import HighlightsNames
from saas.urls.namespaces import Namespace
from utils.lang import ExtendedEnum
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class HighlightKeywordPermissionsApiViewTestCase(ExtendedAPITestCase):

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


class HighlightKeywordBaseApiViewTestCase(ExtendedAPITestCase, ESTestCase):

    def setUp(self):
        super(HighlightKeywordBaseApiViewTestCase, self).setUp()
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_highlights")


class HighlightKeywordAggregationsApiViewTestCase(HighlightKeywordBaseApiViewTestCase):

    def test_aggregations_no_aggregations(self):
        url = get_url()
        response = self.client.get(url)

        self.assertIn("aggregations", response.data)
        self.assertIsNone(response.data["aggregations"])

    def test_aggregations_empty(self):
        url = get_url(size=0, aggregations=",".join(AllowedAggregations.values()))
        response = self.client.get(url)

        self.assertIn("aggregations", response.data)
        expected_aggregations = {
            agg: dict(buckets=[], doc_count_error_upper_bound=0, sum_other_doc_count=0)
            for agg in AllowedAggregations.values()
        }
        self.assertEqual(expected_aggregations, response.data["aggregations"])

    def test_aggregations_categories(self):
        category = "Music"
        keyword = Keyword(id=next(int_iterator))
        keyword.populate_general_data(category=category)
        KeywordManager(Sections.GENERAL_DATA).upsert([keyword])

        url = get_url(size=0, aggregations=AllowedAggregations.CATEGORY.value)
        response = self.client.get(url)

        self.assertIn("aggregations", response.data)
        self.assertIn("general_data.category", response.data["aggregations"])
        self.assertEqual(
            [dict(key=category, doc_count=1)],
            response.data["aggregations"]["general_data.category"]["buckets"]
        )

    def test_aggregations_languages(self):
        language = "English"
        keyword = Keyword(id=next(int_iterator))
        keyword.populate_general_data(language=language)
        KeywordManager(Sections.GENERAL_DATA).upsert([keyword])

        url = get_url(size=0, aggregations=AllowedAggregations.LANGUAGE.value)
        response = self.client.get(url)

        self.assertIn("aggregations", response.data)
        self.assertIn("general_data.language", response.data["aggregations"])
        self.assertEqual(
            [dict(key=language, doc_count=1)],
            response.data["aggregations"]["general_data.language"]["buckets"]
        )


class HighlightKeywordItemsApiViewTestCase(HighlightKeywordBaseApiViewTestCase):

    def test_no_items(self):
        url = get_url(size=0)
        response = self.client.get(url)

        self.assertIn("items", response.data)
        self.assertEqual([], response.data["items"])

    def test_items_page_size(self):
        page_size = 20
        keywords = [Keyword(id=next(int_iterator)) for _ in range(page_size + 1)]
        KeywordManager(Sections.GENERAL_DATA).upsert(keywords)

        url = get_url(page=1, sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)
        response = self.client.get(url)

        self.assertEqual(
            page_size,
            len(response.data["items"])
        )

    def test_max_items(self):
        max_page = 5
        page_size = 20
        total_items = page_size * max_page + 1
        keywords = [Keyword(id=next(int_iterator)) for _ in range(total_items)]
        KeywordManager(Sections.GENERAL_DATA).upsert(keywords)

        url = get_url(page=1, sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)
        response = self.client.get(url)

        self.assertEqual(
            max_page,
            response.data["max_page"]
        )

    def test_filter_languages(self):
        language = "lang"
        keywords = [Keyword(id=next(int_iterator)) for _ in range(2)]
        keywords[0].populate_general_data(language=language)
        KeywordManager(Sections.GENERAL_DATA).upsert(keywords)

        url = get_url(**{AllowedAggregations.LANGUAGE.value: language})
        response = self.client.get(url)

        self.assertEqual(1, response.data["items_count"])
        self.assertEqual(keywords[0].main.id, response.data["items"][0]["main"]["id"])


class AllowedAggregations(ExtendedEnum):
    CATEGORY = "general_data.category"
    LANGUAGE = "general_data.language"


class AllowedSorts(ExtendedEnum):
    VIEWS_30_DAYS_DESC = "stats.last_30day_views:desc"


def get_url(**kwargs):
    return reverse(HighlightsNames.KEYWORDS, [Namespace.HIGHLIGHTS],
                   query_params=kwargs)
