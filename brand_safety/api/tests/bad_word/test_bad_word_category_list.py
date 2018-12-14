from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadWordCategoriesListTestCase(ExtendedAPITestCase):
    def _request(self):
        url = reverse(
            PathNames.BadWord.CATEGORY_LIST,
            [Namespace.BRAND_SAFETY],
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
        self.assertEqual(list(response.data), [])

    def test_categories_list(self):
        self.create_admin_user()
        test_category_1 = "Test category 1"
        test_category_2 = "Test category 2"

        BadWord.objects.create(id=next(int_iterator), name="Bad Word 1", category=test_category_1)
        BadWord.objects.create(id=next(int_iterator), name="Bad Word 2", category=test_category_2)
        BadWord.objects.create(id=next(int_iterator), name="Bad Word 3", category=test_category_1)

        response = self._request()

        self.assertEqual(list(response.data), sorted([test_category_1, test_category_2]))
