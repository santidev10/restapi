from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models import Channel
from highlights.api.urls.names import HighlightsNames
from saas.urls.namespaces import Namespace
from utils.lang import ExtendedEnum
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class HighlightChannelPermissionsApiViewTestCase(ExtendedAPITestCase):

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


class HighlightChannelAggregationsApiViewTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_highlights")

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
            f"{agg}:count": []
            for agg in AllowedAggregations.values()
        }
        self.assertEqual(expected_aggregations, response.data["aggregations"])

    def test_aggregations_categories(self):
        category = "Music"
        channel = Channel(id=next(int_iterator))
        channel.populate_general_data(top_category=category)
        ChannelManager(Sections.GENERAL_DATA).upsert([channel])

        url = get_url(size=0, aggregations=AllowedAggregations.CATEGORY.value)
        response = self.client.get(url)

        self.assertIn("aggregations", response.data)
        self.assertIn("category:count", response.data["aggregations"])
        self.assertEqual(
            [category, 1],
            response.data["aggregations"]["category:count"]
        )

    def test_aggregations_languages(self):
        language = "English"
        channel = Channel(id=next(int_iterator))
        channel.populate_general_data(top_language=language)
        ChannelManager(Sections.GENERAL_DATA).upsert([channel])

        url = get_url(size=0, aggregations=AllowedAggregations.LANGUAGE.value)
        response = self.client.get(url)

        self.assertIn("aggregations", response.data)
        self.assertIn("language:count", response.data["aggregations"])
        self.assertEqual(
            [language, 1],
            response.data["aggregations"]["language:count"]
        )


class HighlightChannelItemsApiViewTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_highlights")

    def test_no_items(self):
        url = get_url(size=0)
        response = self.client.get(url)

        self.assertIn("items", response.data)
        self.assertEqual([], response.data["items"])

    def test_items_page_size(self):
        page_size = 20
        channels = [Channel(id=next(int_iterator)) for _ in range(page_size + 1)]
        ChannelManager(Sections.GENERAL_DATA).upsert(channels)

        url = get_url(page=1, sort=AllowedSorts.VIEWS_30_DAYS_DESC)
        response = self.client.get(url)

        self.assertEqual(
            page_size,
            len(response.data["items"])
        )

    def test_max_items(self):
        max_page = 5
        page_size = 20
        total_items = page_size*max_page + 1
        channels = [Channel(id=next(int_iterator)) for _ in range(total_items)]
        ChannelManager(Sections.GENERAL_DATA).upsert(channels)

        url = get_url(page=1, sort=AllowedSorts.VIEWS_30_DAYS_DESC)
        response = self.client.get(url)

        self.assertEqual(
            max_page,
            response.data["max_page"]
        )


class AllowedAggregations(ExtendedEnum):
    CATEGORY = "category"
    LANGUAGE = "language"


class AllowedSorts(ExtendedEnum):
    VIEWS_30_DAYS_DESC = "thirty_days_views:desc"


def get_url(**kwargs):
    return reverse(HighlightsNames.CHANNELS, [Namespace.HIGHLIGHTS],
                   query_params=kwargs)
