from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadWordDeleteTestCase(ExtendedAPITestCase):
    def _get_url(self, pk=None):
        pk = pk or self.bad_word.id
        return reverse(
            PathNames.BadWord.UPDATE_DELETE,
            [Namespace.BRAND_SAFETY],
            args=(pk,)
        )

    def setUp(self):
        self.bad_word = BadWord.objects.create(
            id=next(int_iterator),
            name="test bad word",
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
        self.assertFalse(BadWord.objects.filter(pk=self.bad_word.pk).exists())

    def test_no_entity(self):
        bad_word = self.bad_word
        bad_word.delete()
        self.create_admin_user()

        response = self._request(bad_word.id)

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
