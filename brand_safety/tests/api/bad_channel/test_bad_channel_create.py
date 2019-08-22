import json

from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadChannel
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadChannelCreateTestCase(ExtendedAPITestCase):
    def _request(self, **bad_channel_data):
        url = reverse(
            PathNames.BadChannel.LIST_AND_CREATE,
            [Namespace.BRAND_SAFETY],
        )
        return self.client.post(url, json.dumps(bad_channel_data), content_type="application/json")

    def test_not_auth(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_no_permissions(self):
        self.create_test_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_has_permissions(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad channel",
            category="Test category",
            thumbnail_url=next_url(),
            youtube_id=str(next(int_iterator)),
        )

        self.assertEqual(response.status_code, HTTP_201_CREATED)

    def test_create_is_not_deleted(self):
        self.create_admin_user()

        response = self._request(
            title="Test",
            category="Test",
            thumbnail_url=next_url(),
            youtube_id=str(next(int_iterator)),
        )

        self.assertEqual(response.status_code, HTTP_201_CREATED)
        new_id = response.data["id"]
        self.assertFalse(BadChannel.objects.get(id=new_id).is_deleted)

    def test_title_required(self):
        self.create_admin_user()

        response = self._request(
            title=None,
            category="Test category",
            thumbnail_url=next_url(),
            youtube_id=str(next(int_iterator)),
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_category_required(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad channel",
            category=None,
            thumbnail_url=next_url(),
            youtube_id=str(next(int_iterator)),
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_thumbnail_required(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad channel",
            category="Test ",
            thumbnail_url=None,
            youtube_id=str(next(int_iterator)),
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_thumbnail_url_validation(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad channel",
            category="Test ",
            thumbnail_url="htp://invalid.url",
            youtube_id=str(next(int_iterator)),
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_youtube_id_required(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad channel",
            category="Test ",
            thumbnail_url=next_url(),
            youtube_id=None,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_allow_the_same_bad_channel_in_different_categories(self):
        self.create_admin_user()
        test_category_1 = "test category 1"
        test_category_2 = "test category 2"

        test_bad_channel = BadChannel.objects.create(
            title="test bad channel",
            category=test_category_1,
            thumbnail_url=next_url(),
            youtube_id=str(next(int_iterator)),
        )

        response = self._request(
            title=test_bad_channel.title,
            category=test_category_2,
            thumbnail_url=test_bad_channel.thumbnail_url,
            youtube_id=test_bad_channel.youtube_id,
        )

        self.assertEqual(response.status_code, HTTP_201_CREATED)

    def test_reject_duplicates(self):
        self.create_admin_user()
        youtube_id = str(next(int_iterator))
        test_bad_channel = BadChannel.objects.create(
            youtube_id=youtube_id,
            title="test bad channel",
            category="test category",
        )

        response = self._request(
            youtube_id=youtube_id,
            category=test_bad_channel.category,
            title="",
            thumbnail_url=next_url(),
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_crete_duplicate_for_removed_item(self):
        self.create_admin_user()
        youtube_id = str(next(int_iterator))
        test_bad_channel = BadChannel.objects.create(
            youtube_id=youtube_id,
            title="test bad video",
            category="test category",
        )
        old_id = test_bad_channel.id
        test_bad_channel.delete()

        response = self._request(
            youtube_id=youtube_id,
            category=test_bad_channel.category,
            title="Test",
            thumbnail_url=next_url(),
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(response.data["id"], old_id)
        test_bad_channel.refresh_from_db()
        self.assertFalse(test_bad_channel.is_deleted)


def next_url():
    return "http://example.com/path/{}".format(next(int_iterator))
