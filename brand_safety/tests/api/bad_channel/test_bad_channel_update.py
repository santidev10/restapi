import json

from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadChannel
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadChannelUpdateTestCase(ExtendedAPITestCase):
    def _get_url(self, pk=None):
        pk = pk or self.bad_channel.id
        return reverse(
            PathNames.BadChannel.UPDATE_DELETE,
            [Namespace.BRAND_SAFETY],
            args=(pk,)
        )

    def setUp(self):
        self.bad_channel = BadChannel.objects.create(
            id=next(int_iterator),
            title="test bad channel",
            category="test category"
        )

    def _request(self, pk=None, **kwargs):
        url = self._get_url(pk)
        return self.client.patch(
            url,
            json.dumps(kwargs),
            content_type="application/json"
        )

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

    def test_no_entity(self):
        bad_channel = self.bad_channel
        bad_channel.delete()
        self.create_admin_user()

        response = self._request(bad_channel.id)

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_title_not_none(self):
        self.create_admin_user()

        response = self._request(
            title=None,
            category="Test category",
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_category_not_none(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad channel",
            category=None,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_youtube_id_not_none(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad channel",
            youtube_id=None,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_thumbnail_not_none(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad channel",
            thumbnail_url=None,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_thumbnail_validation(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad channel",
            thumbnail_url="htp://invalid.url",
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_allow_the_same_bad_channel_in_different_categories(self):
        self.create_admin_user()
        bad_channel = self.bad_channel
        another_bad_channel = BadChannel.objects.create(
            id=next(int_iterator),
            title="test bad channel #2",
            category="test category #2",
        )

        response = self._request(
            pk=bad_channel.pk,
            title=another_bad_channel.title,
        )

        self.assertEqual(response.status_code, HTTP_200_OK)
        bad_channel.refresh_from_db()
        self.assertEqual(bad_channel.title, another_bad_channel.title)
        self.assertNotEqual(bad_channel.category, another_bad_channel.category)

    def test_reject_duplicates(self):
        self.create_admin_user()
        bad_channel = self.bad_channel
        another_bad_channel = BadChannel.objects.create(
            id=next(int_iterator),
            title="test bad channel #2",
            category="test category #2",
        )

        response = self._request(
            pk=bad_channel.pk,
            title=another_bad_channel.title,
            category=another_bad_channel.category,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_update_deleted(self):
        self.create_admin_user()
        bad_channel = self.bad_channel
        bad_channel.is_deleted = True
        bad_channel.save()

        response = self._request(
            pk=bad_channel.pk,
            title=bad_channel.title,
            category=bad_channel.category,
        )

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
