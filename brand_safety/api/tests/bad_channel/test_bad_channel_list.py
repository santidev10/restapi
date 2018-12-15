from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadChannel
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadChannelListTestCase(ExtendedAPITestCase):
    def _request(self, **query_params):
        url = reverse(
            PathNames.BadChannel.LIST_AND_CREATE,
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
        bad_channel = BadChannel.objects.create(
            id=next(int_iterator),
            category="Test category",
            title="Test Bad Channel"
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
        self.assertEqual(item["id"], bad_channel.id)
        self.assertEqual(item["title"], bad_channel.title)
        self.assertEqual(item["category"], bad_channel.category)

    def test_ordered_by_name(self):
        test_category = "test_category"
        bad_channels = [
            "bad_channel_2",
            "bad_channel_3",
            "bad_channel_1",
        ]
        self.assertNotEqual(bad_channels, sorted(bad_channels))
        for bad_channel in bad_channels:
            BadChannel.objects.create(
                id=next(int_iterator),
                title=bad_channel,
                youtube_id=str(next(int_iterator)),
                category=test_category,
            )
        self.create_admin_user()

        response = self._request()

        response_bad_channels = [item["title"] for item in response.data["items"]]
        self.assertEqual(response_bad_channels, sorted(bad_channels))

    def test_filter_by_category(self):
        test_category = "Test category"
        BadChannel.objects.create(id=next(int_iterator), title="Bad Channel 1",
                                  category=test_category + " test suffix")
        expected_bad_channel = BadChannel.objects.create(id=next(int_iterator), title="Bad Channel 2",
                                                         category=test_category)

        self.create_admin_user()

        response = self._request(category=test_category)

        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], expected_bad_channel.id)

    def test_search_positive(self):
        test_bad_channel = "test bad channel"
        test_search_query = test_bad_channel[3:-3].upper()
        BadChannel.objects.create(id=next(int_iterator), title=test_bad_channel, category="")

        self.create_admin_user()

        response = self._request(title=test_search_query)

        self.assertEqual(response.data["items_count"], 1)

    def test_search_negative(self):
        test_bad_channel = "bad channel 1"
        test_search_query = "bad channel 2"
        BadChannel.objects.create(id=next(int_iterator), title=test_bad_channel, category="")

        self.create_admin_user()

        response = self._request(search=test_search_query)

        self.assertEqual(response.data["items_count"], 0)

    def test_exclude_deleted(self):
        bad_channel = BadChannel.objects.create(id=next(int_iterator), title="", category="")
        bad_channel.delete()

        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.data["items_count"], 0)
