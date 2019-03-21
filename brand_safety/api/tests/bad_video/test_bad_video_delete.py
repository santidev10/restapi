from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadVideo
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadVideoDeleteTestCase(ExtendedAPITestCase):
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

    def _request(self, pk=None):
        url = self._get_url(pk)
        return self.client.delete(url)

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

        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)
        self.assertFalse(BadVideo.objects.filter(pk=self.bad_video.pk).exists())

    def test_marks_as_deleted(self):
        self.create_admin_user()

        self._request()

        self.assertTrue(BadVideo.objects.get_base_queryset().filter(pk=self.bad_video.pk).exists())
        self.assertTrue(BadVideo.objects.get_base_queryset().get(pk=self.bad_video.pk).is_deleted)

    def test_no_entity(self):
        bad_video = self.bad_video
        bad_video.delete()
        self.create_admin_user()

        response = self._request(bad_video.id)

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
