from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadVideo
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadVideoListTestCase(ExtendedAPITestCase):
    def _request(self, **query_params):
        url = reverse(
            PathNames.BadVideo.LIST_AND_CREATE,
            [Namespace.BRAND_SAFETY],
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

    def test_has_permissions(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_is_paged(self):
        self.create_admin_user()

        response = self._request()

        empty_response = dict(
            current_page=1,
            items=[],
            items_count=0,
            max_page=1,
        )
        self.assertEqual(response.data, empty_response)

    def test_serialization(self):
        bad_video = BadVideo.objects.create(
            id=next(int_iterator),
            category="Test category",
            title="Test Bad Word"
        )
        self.create_admin_user()

        response = self._request()
        data = response.data

        self.assertEqual(data["items_count"], 1)
        item = data["items"][0]
        self.assertEqual(set(item.keys()), {
            "category",
            "id",
            "title",
            "reason",
            "thumbnail_url",
            "youtube_id",
        })
        self.assertEqual(item["id"], bad_video.id)
        self.assertEqual(item["title"], bad_video.title)
        self.assertEqual(item["category"], bad_video.category)

    def test_ordered_by_name(self):
        test_category = "test_category"
        bad_videos = [
            "bad_video_2",
            "bad_video_3",
            "bad_video_1",
        ]
        self.assertNotEqual(bad_videos, sorted(bad_videos))
        for bad_video in bad_videos:
            BadVideo.objects.create(
                id=next(int_iterator),
                title=bad_video,
                youtube_id=str(next(int_iterator)),
                category=test_category,
            )
        self.create_admin_user()

        response = self._request()

        response_bad_videos = [item["title"] for item in response.data["items"]]
        self.assertEqual(response_bad_videos, sorted(bad_videos))

    def test_filter_by_category(self):
        test_category = "Test category"
        BadVideo.objects.create(id=next(int_iterator), title="Bad Word 1", category=test_category + " test suffix")
        expected_bad_video = BadVideo.objects.create(id=next(int_iterator), title="Bad Word 2",
                                                     category=test_category)

        self.create_admin_user()

        response = self._request(category=test_category)

        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], expected_bad_video.id)

    def test_search_positive(self):
        test_bad_video = "test bad video"
        test_search_query = test_bad_video[3:-3].upper()
        BadVideo.objects.create(id=next(int_iterator), title=test_bad_video, category="")

        self.create_admin_user()

        response = self._request(title=test_search_query)

        self.assertEqual(response.data["items_count"], 1)

    def test_search_negative(self):
        test_bad_video = "bad video 1"
        test_search_query = "bad video 2"
        BadVideo.objects.create(id=next(int_iterator), title=test_bad_video, category="")

        self.create_admin_user()

        response = self._request(search=test_search_query)

        self.assertEqual(response.data["items_count"], 0)

    def test_exclude_deleted(self):
        bad_video = BadVideo.objects.create(id=next(int_iterator), title="", category="")
        bad_video.delete()

        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.data["items_count"], 0)