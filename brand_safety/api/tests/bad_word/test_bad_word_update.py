import json

from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord, BadWordCategory
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadWordUpdateTestCase(ExtendedAPITestCase):
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
        bad_word = self.bad_word
        bad_word.delete()
        self.create_admin_user()

        response = self._request(bad_word.id)

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_name_required(self):
        self.create_admin_user()

        response = self._request(
            name=None,
            category="Test category",
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_category_required(self):
        self.create_admin_user()

        response = self._request(
            name="Test bad word",
            category=None,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_allow_the_same_bad_word_in_different_categories(self):
        self.create_admin_user()
        bad_word = self.bad_word
        another_bad_word = BadWord.objects.create(
            id=next(int_iterator),
            name="test bad word #2",
            category="test category #2",
        )

        response = self._request(
            pk=bad_word.pk,
            name=another_bad_word.name,
        )

        self.assertEqual(response.status_code, HTTP_200_OK)
        bad_word.refresh_from_db()
        self.assertEqual(bad_word.name, another_bad_word.name)
        self.assertNotEqual(bad_word.category, another_bad_word.category)

    def test_reject_duplicates(self):
        self.create_admin_user()
        bad_word = self.bad_word
        another_bad_word = BadWord.objects.create(
            id=next(int_iterator),
            name="test bad word #2",
            category="test category #2",
        )

        response = self._request(
            pk=bad_word.pk,
            name=another_bad_word.name,
            category=another_bad_word.category,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_update_changes_category_ref(self):
        self.create_admin_user()
        bad_word = self.bad_word
        new_category = BadWordCategory.objects.create(name="new category")
        response = self._request(
            id=bad_word.id,
            name=bad_word.name,
            category=new_category.name
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        bad_word.refresh_from_db()
        self.assertEqual(bad_word.category_ref.id, new_category.id)
        self.assertEqual(bad_word.category_ref.name, new_category.name)
