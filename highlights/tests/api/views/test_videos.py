from time import sleep

from django.contrib.auth.models import Group
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from highlights.api.urls.names import HighlightsNames
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from utils.lang import ExtendedEnum
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class HighlightVideoPermissionsApiViewTestCase(ExtendedAPITestCase, ESTestCase):

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


class HighlightVideoBaseApiViewTestCase(ExtendedAPITestCase, ESTestCase):

    def setUp(self):
        super(HighlightVideoBaseApiViewTestCase, self).setUp()
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_highlights")


class HighlightVideoAggregationsApiViewTestCase(HighlightVideoBaseApiViewTestCase):

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
        video = Video(id=next(int_iterator))
        video.populate_general_data(category=category)
        VideoManager(Sections.GENERAL_DATA).upsert([video])

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
        video = Video(id=next(int_iterator))
        video.populate_general_data(language=language)
        VideoManager(Sections.GENERAL_DATA).upsert([video])

        url = get_url(size=0, aggregations=AllowedAggregations.LANGUAGE.value)
        response = self.client.get(url)

        self.assertIn("aggregations", response.data)
        self.assertIn("general_data.language", response.data["aggregations"])
        self.assertEqual(
            [dict(key=language, doc_count=1)],
            response.data["aggregations"]["general_data.language"]["buckets"]
        )

    def test_language_top_ten(self):
        language_limit = 10
        videos = [Video(id=next(int_iterator)) for _ in range(language_limit + 1)]
        for i, video in enumerate(videos):
            video.populate_general_data(language=f"lang_{i}")
        VideoManager(Sections.GENERAL_DATA).upsert(videos)

        url = get_url(size=0, aggregations=AllowedAggregations.LANGUAGE.value)
        response = self.client.get(url)

        self.assertEqual(
            language_limit,
            len(response.data["aggregations"]["general_data.language"]["buckets"])
        )


class HighlightVideoItemsApiViewTestCase(HighlightVideoBaseApiViewTestCase):

    def test_no_items(self):
        url = get_url(size=0)
        response = self.client.get(url)

        self.assertIn("items", response.data)
        self.assertEqual([], response.data["items"])

    def test_items_page_size(self):
        page_size = 20
        videos = [Video(id=next(int_iterator)) for _ in range(page_size + 1)]
        VideoManager(Sections.GENERAL_DATA).upsert(videos)

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
        videos = [Video(id=next(int_iterator)) for _ in range(total_items)]
        VideoManager(Sections.GENERAL_DATA).upsert(videos)

        url = get_url(page=1, sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)
        response = self.client.get(url)

        self.assertEqual(
            max_page,
            response.data["max_page"]
        )

    def test_filter_languages(self):
        language = "lang"
        videos = [Video(id=next(int_iterator)) for _ in range(2)]
        videos[0].populate_general_data(language=language)
        VideoManager(Sections.GENERAL_DATA).upsert(videos)

        url = get_url(**{AllowedAggregations.LANGUAGE.value: language})
        response = self.client.get(url)

        self.assertEqual(1, response.data["items_count"])
        self.assertEqual(videos[0].main.id, response.data["items"][0]["main"]["id"])

    def test_brand_safety(self):
        user = self.create_admin_user()
        Group.objects.get_or_create(name=PermissionGroupNames.BRAND_SAFETY_SCORING)
        user.add_custom_user_permission("video_list")
        user.add_custom_user_group(PermissionGroupNames.BRAND_SAFETY_SCORING)
        video_id = str(next(int_iterator))
        score = 92
        video = Video(**{
            "meta": {
                "id": video_id
            },
            "brand_safety": {
                "overall_score": score
            }
        })
        sleep(1)
        VideoManager(upsert_sections=[Sections.GENERAL_DATA, Sections.BRAND_SAFETY]).upsert([video])
        response = self.client.get(get_url())
        self.assertEqual(
            score,
            response.data["items"][0]["brand_safety"]["overall_score"]
        )

    def test_sorting_30day_views(self):
        views = [1, 3, 2]
        keywords = [Video(next(int_iterator)) for _ in range(len(views))]
        for keyword, item_views in zip(keywords, views):
            keyword.populate_stats(last_30day_views=item_views)
        VideoManager(sections=[Sections.GENERAL_DATA, Sections.STATS]).upsert(keywords)

        url = get_url(sort=AllowedSorts.VIEWS_30_DAYS_DESC.value)
        response = self.client.get(url)

        response_views = [item["stats"]["last_30day_views"] for item in response.data["items"]]
        self.assertEqual(
            list(sorted(views, reverse=True)),
            response_views
        )

    def test_extra_fields(self):
        self.create_admin_user()
        extra_fields = ("brand_safety_data", "chart_data", "transcript", "blacklist_data")
        video = Video(str(next(int_iterator)))
        VideoManager([Sections.GENERAL_DATA]).upsert([video])

        url = get_url()
        response = self.client.get(url)

        for field in extra_fields:
            with self.subTest(field):
                self.assertIn(field, response.data["items"][0])


class AllowedAggregations(ExtendedEnum):
    CATEGORY = "general_data.category"
    LANGUAGE = "general_data.language"


class AllowedSorts(ExtendedEnum):
    VIEWS_30_DAYS_DESC = "stats.last_30day_views:desc"


def get_url(**kwargs):
    return reverse(HighlightsNames.VIDEOS, [Namespace.HIGHLIGHTS],
                   query_params=kwargs)
