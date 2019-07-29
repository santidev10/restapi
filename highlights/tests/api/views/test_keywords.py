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
        KeywordManager(Sections.MAIN).upsert([keyword])

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
        KeywordManager(Sections.MAIN).upsert([keyword])

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
        KeywordManager(Sections.MAIN).upsert(keywords)

        url = get_url(page=1, sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)
        response = self.client.get(url)

        self.assertEqual(
            page_size,
            len(response.data["items"])
        )

    def test_items_offset(self):
        page_size = 20
        next_id = lambda: ("0" * 5 + str(next(int_iterator)))[-5:]
        keywords = [Keyword(id=next_id()) for _ in range(2 * page_size + 1)]
        KeywordManager(Sections.MAIN).upsert(keywords)

        url = get_url(page=2, sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)
        response = self.client.get(url)

        self.assertEqual(
            page_size,
            len(response.data["items"])
        )
        expected_ids = [item.main.id for item in keywords[page_size:page_size * 2]]
        self.assertEqual(
            expected_ids,
            [item["main"]["id"] for item in response.data["items"]]
        )

    def test_max_items(self):
        max_page = 5
        page_size = 20
        total_items = page_size * max_page + 1
        keywords = [Keyword(id=next(int_iterator)) for _ in range(total_items)]
        KeywordManager(Sections.MAIN).upsert(keywords)

        url = get_url(page=1, sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)
        response = self.client.get(url)

        self.assertEqual(
            max_page,
            response.data["max_page"]
        )

    def test_filter_category(self):
        category = "category"
        keywords = [Keyword(id=next(int_iterator)) for _ in range(2)]
        keywords[0].populate_stats(top_category=category)
        KeywordManager(Sections.STATS).upsert(keywords)

        url = get_url(**{AllowedAggregations.CATEGORY.value: category})
        response = self.client.get(url)

        self.assertEqual(1, response.data["items_count"])
        self.assertEqual(keywords[0].main.id, response.data["items"][0]["main"]["id"])

    def test_sorting(self):
        views = [1, 3, 2]
        keywords = [Keyword(next(int_iterator)) for _ in range(len(views))]
        for keyword, item_views in zip(keywords, views):
            keyword.populate_stats(last_30day_views=item_views)
        KeywordManager(Sections.STATS).upsert(keywords)

        url = get_url(sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)
        response = self.client.get(url)

        response_views = [item["stats"]["last_30day_views"] for item in response.data["items"]]
        self.assertEqual(
            list(sorted(views, reverse=True)),
            response_views
        )


class AllowedAggregations(ExtendedEnum):
    CATEGORY = "general_data.top_category"


class AllowedSorts(ExtendedEnum):
    VIEWS_30_DAYS_DESC = "stats.last_30day_views:desc"


def get_url(**kwargs):
    return reverse(HighlightsNames.KEYWORDS, [Namespace.HIGHLIGHTS],
                   query_params=kwargs)
