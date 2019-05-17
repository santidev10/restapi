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


class BadWordListTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.test_category = BadWordCategory.objects.create(name="testing")

    def _request(self, **query_params):
        url = reverse(
            PathNames.BadWord.LIST_AND_CREATE,
            [Namespace.BRAND_SAFETY],
            query_params=query_params,
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

    def test_serialization(self):
        bad_word = BadWord.objects.create(
            id=next(int_iterator),
            category=self.test_category,
            name="Test Bad Word"
        )
        self.create_admin_user()

        response = self._request()
        data = response.data

        self.assertEqual(set(data[0].keys()), {"id", "name", "category", "negative_score"})
        self.assertEqual(data[0]["id"], bad_word.id)
        self.assertEqual(data[0]["name"], bad_word.name)
        self.assertEqual(data[0]["category"], bad_word.category.name)

    def test_ordered_by_name(self):
        bad_words = [
            "bad_word_2",
            "bad_word_3",
            "bad_word_1",
        ]
        self.assertNotEqual(bad_words, sorted(bad_words))
        for bad_word in bad_words:
            BadWord.objects.create(
                id=next(int_iterator),
                name=bad_word,
                category=self.test_category,
            )
        self.create_admin_user()

        response = self._request()

        response_bad_words = [item["name"] for item in response.data]
        self.assertEqual(response_bad_words, sorted(bad_words))

    def test_filter_by_category(self):
        category_2 = BadWordCategory.objects.create(name="testing test suffix")
        BadWord.objects.create(id=next(int_iterator), name="Bad Word 1", category=category_2)
        expected_bad_word = BadWord.objects.create(id=next(int_iterator), name="Bad Word 2", category=self.test_category)

        self.create_admin_user()

        response = self._request(category__name=self.test_category.name)

        self.assertEqual(response.data[0]["id"], expected_bad_word.id)

    def test_search_positive(self):
        test_bad_word = "test bad word"
        test_search_query = test_bad_word[3:-3].upper()
        BadWord.objects.create(id=next(int_iterator), name=test_bad_word, category=self.test_category)

        self.create_admin_user()

        response = self._request(name=test_search_query)

        self.assertEqual(len(response.data), 1)

    def test_search_negative(self):
        test_bad_word = "bad word 1"
        test_search_query = "bad word 2"
        BadWord.objects.create(id=next(int_iterator), name=test_bad_word, category=self.test_category)

        self.create_admin_user()

        response = self._request(search=test_search_query)

        self.assertEqual(len(response.data), 0)
