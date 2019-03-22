import json

from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadWordCreateTestCase(ExtendedAPITestCase):
    def _request(self, **bad_word_data):
        url = reverse(
            PathNames.BadWord.LIST_AND_CREATE,
            [Namespace.BRAND_SAFETY],
        )
        return self.client.post(url, json.dumps(bad_word_data), content_type="application/json")

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
            name="Test bad word",
            category="Test category",
        )

        self.assertEqual(response.status_code, HTTP_201_CREATED)

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
        test_category_1 = "test category 1"
        test_category_2 = "test category 2"
        test_bad_word = BadWord.objects.create(name="test bad word", category=test_category_1)

        response = self._request(
            name=test_bad_word.name,
            category=test_category_2,
        )

        self.assertEqual(response.status_code, HTTP_201_CREATED)

    def test_reject_duplicates(self):
        self.create_admin_user()
        test_bad_word = BadWord.objects.create(name="test bad word", category="test category")

        response = self._request(
            name=test_bad_word.name,
            category=test_bad_word.category,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
