from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
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
        test_category_1 = BadWordCategory.objects.create(id=1, name="Test category 1")
        test_category_2 = BadWordCategory.objects.create(id=2, name="Test category 2")
        test_category_1_expected_item = {
            "id": test_category_1.id,
            "name": test_category_1.name
        }
        test_category_2_expected_item = {
            "id": test_category_2.id,
            "name": test_category_2.name
        }
        response = self._request()
        self.assertEqual(
            sorted(list(response.data), key=lambda x: x["name"]),
            [test_category_1_expected_item, test_category_2_expected_item]
        )
        self.assertEqual(len(response.data), 2)