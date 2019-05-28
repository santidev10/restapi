import json

from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from brand_safety.models import BadWordLanguage
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

    def test_reject_duplicates(self):
        self.create_admin_user()
        test_category = BadWordCategory.objects.create(name="test category")
        test_bad_word = BadWord.objects.create(name="test bad word", category=test_category)

        response = self._request(
            name=test_bad_word.name,
            category=test_bad_word.name,
        )

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_category_create_category(self):
        self.create_admin_user()
        test_category = BadWordCategory.objects.create(name="test category")
        test_bad_word = "testing"
        response = self._request(
            name=test_bad_word,
            category=test_category.id,
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        test_bad_word_obj = BadWord.objects.get(name=test_bad_word)
        test_bad_word_category = BadWordCategory.objects.get(name=test_category.name)
        self.assertEqual(test_bad_word_obj.category_id, test_bad_word_category.id)

    def test_category_create_existing_category(self):
        self.create_admin_user()
        test_bad_word = "testing"
        test_category = BadWordCategory.objects.create(name="testing")
        response = self._request(
            name=test_bad_word,
            category=test_category.id,
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(response.data["category"], test_category.name)
        test_bad_word_obj = BadWord.objects.get(name=test_bad_word)
        self.assertEqual(test_bad_word_obj.category_id, test_category.id)
        self.assertEqual(test_bad_word_obj.category.name, test_category.name)

    def test_bad_word_name_strip(self):
        self.create_admin_user()
        test_bad_word = "testing"
        test_bad_word_space = "testing "
        test_category = BadWordCategory.objects.create(name="testing")
        response = self._request(
            name=test_bad_word_space,
            category=test_category.id,
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(response.data["name"], test_bad_word)

    def test_bad_word_name_lower(self):
        self.create_admin_user()
        test_bad_word = "testing"
        test_bad_word_upper = "TeStIng"
        test_category = BadWordCategory.objects.create(name="testing")
        response = self._request(
            name=test_bad_word_upper,
            category=test_category.id,
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(response.data["name"], test_bad_word)

    def test_bad_word_name_strip_lower(self):
        self.create_admin_user()
        test_bad_word = "test word"
        test_bad_word_space_upper = " Test Word  "
        test_category = BadWordCategory.objects.create(name="testing")
        response = self._request(
            name=test_bad_word_space_upper,
            category=test_category.id,
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(response.data["name"], test_bad_word)

    def test_bad_word_default_language(self):
        self.create_admin_user()
        test_bad_word = "testing"
        test_category = BadWordCategory.objects.create(name="testing")
        response = self._request(
            name=test_bad_word,
            category=test_category.id,
        )
        from_db = BadWord.objects.get(name=test_bad_word)
        self.assertEqual(from_db.language.name, BadWordLanguage.DEFAULT)

