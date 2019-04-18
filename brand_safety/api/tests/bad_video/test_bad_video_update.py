import json

from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadVideo
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadVideoUpdateTestCase(ExtendedAPITestCase):
    def _get_url(self, pk=None):
        pk = pk or self.bad_video.id
        return reverse(
            PathNames.BadVideo.UPDATE_DELETE,
            [Namespace.BRAND_SAFETY],
            args=(pk,)
        )

    def setUp(self):
        self.bad_video = BadVideo.objects.create(
            id=next(int_iterator),
            title="test bad video",
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
        bad_video = self.bad_video
        bad_video.delete()
        self.create_admin_user()

        response = self._request(bad_video.id)

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
            title="Test bad video",
            category=None,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_youtube_id_not_none(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad video",
            youtube_id=None,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_thumbnail_not_none(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad video",
            thumbnail_url=None,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_thumbnail_validation(self):
        self.create_admin_user()

        response = self._request(
            title="Test bad video",
            thumbnail_url="htp://invalid.url",
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_allow_the_same_bad_video_in_different_categories(self):
        self.create_admin_user()
        bad_video = self.bad_video
        another_bad_video = BadVideo.objects.create(
            id=next(int_iterator),
            title="test bad video #2",
            category="test category #2",
        )

        response = self._request(
            pk=bad_video.pk,
            title=another_bad_video.title,
        )

        self.assertEqual(response.status_code, HTTP_200_OK)
        bad_video.refresh_from_db()
        self.assertEqual(bad_video.title, another_bad_video.title)
        self.assertNotEqual(bad_video.category, another_bad_video.category)

    def test_reject_duplicates(self):
        self.create_admin_user()
        bad_video = self.bad_video
        another_bad_video = BadVideo.objects.create(
            id=next(int_iterator),
            title="test bad video #2",
            category="test category #2",
        )

        response = self._request(
            pk=bad_video.pk,
            title=another_bad_video.title,
            category=another_bad_video.category,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_update_deleted(self):
        self.create_admin_user()
        bad_video = self.bad_video
        bad_video.is_deleted = True
        bad_video.save()

        response = self._request(
            pk=bad_video.pk,
            title=bad_video.title,
            category=bad_video.category,
        )

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
