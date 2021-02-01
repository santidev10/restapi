from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.models import AuditLanguage
from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class BadWordListTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.test_category = BadWordCategory.objects.create(name="testing")
        self.test_language = AuditLanguage.objects.create(language="sv")
        self.words = [
            {
                "name": "test1",
                "category": BadWordCategory.from_string("profanity"),
                "language": AuditLanguage.from_string("en")
            },
            {
                "name": "test2",
                "category": BadWordCategory.from_string("terrorism"),
                "language": AuditLanguage.from_string("en")
            },
            {
                "name": "test3",
                "category": BadWordCategory.from_string("terrorism"),
                "language": AuditLanguage.from_string("en")
            },
            {
                "name": "test4",
                "category": BadWordCategory.from_string("drugs"),
                "language": AuditLanguage.from_string("sv")
            }
        ]

    def _request(self, **query_params):
        query_params["page"] = 1
        url = reverse(
            PathNames.BadWord.LIST_AND_CREATE,
            [Namespace.BRAND_SAFETY],
            query_params=query_params,
        )
        return self.client.get(url)

    def test_bad_word_manager_objects(self):
        BadWord.objects.create(**self.words[0])
        word2 = BadWord(**self.words[1])
        word2.deleted_at = timezone.now()
        word2.save()

        self.assertEqual(1, BadWord.objects.all().count())
        self.assertEqual(2, BadWord.all_objects.all().count())

    def test_not_auth(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_no_permissions(self):
        self.create_test_user()
        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_has_permissions(self):
        self.create_test_user(perms={
            StaticPermissions.BSTE: True,
        })
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_serialization(self):
        self.create_admin_user()
        bad_word = BadWord.objects.create(
            id=next(int_iterator),
            category=self.test_category,
            name="Test Bad Word",
            meta_scoring=False,
            comment_scoring=True
        )
        response = self._request()
        data = response.data["items"]

        self.assertEqual(set(data[0].keys()), {"id", "name", "category", "negative_score", "language", "meta_scoring",
                                               "comment_scoring"})
        self.assertEqual(data[0]["id"], bad_word.id)
        self.assertEqual(data[0]["name"], bad_word.name)
        self.assertEqual(data[0]["category"], bad_word.category.name)
        self.assertEqual(data[0]["meta_scoring"], bad_word.meta_scoring)
        self.assertEqual(data[0]["comment_scoring"], bad_word.comment_scoring)

    def test_ordered_by_name(self):
        self.create_admin_user()
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
        response = self._request()
        response_bad_words = [item["name"] for item in response.data["items"]]
        self.assertEqual(response_bad_words, sorted(bad_words))

    def test_filter_by_category(self):
        self.create_admin_user()
        category_2 = BadWordCategory.objects.create(name="testing test suffix")
        BadWord.objects.create(id=next(int_iterator), name="Bad Word 1", category=category_2)
        expected_bad_word = BadWord.objects.create(
            id=next(int_iterator),
            name="Bad Word 2",
            category=self.test_category
        )
        response = self._request(category=self.test_category.id)
        self.assertEqual(response.data["items"][0]["id"], expected_bad_word.id)

    def test_filter_by_language(self):
        self.create_admin_user()
        BadWord.objects.bulk_create([BadWord(**opts) for opts in self.words])
        response = self._request(language="en")
        data = response.data["items"]

        self.assertEqual(len(self.words) - 1, len(data))
        self.assertEqual(True, all([word["language"] == "en" for word in data]))

    def test_filter_by_search(self):
        self.create_admin_user()
        BadWord.objects.bulk_create([BadWord(**opts) for opts in self.words])

        response1 = self._request(search="est")
        data1 = response1.data["items"]
        self.assertEqual(len(self.words), len(data1))

        response2 = self._request(search="test1")
        data2 = response2.data["items"]
        self.assertEqual(1, len(data2))
        self.assertEqual("test1", data2[0]["name"])

    def test_search_positive(self):
        self.create_admin_user()
        test_bad_word = "test bad word"
        test_search_query = test_bad_word[3:-3].upper()
        BadWord.objects.create(id=next(int_iterator), name=test_bad_word, category=self.test_category)
        response = self._request(name=test_search_query)

        self.assertEqual(len(response.data["items"]), 1)

    def test_search_negative(self):
        self.create_admin_user()
        test_bad_word = "bad word 1"
        test_search_query = "bad word 2"
        BadWord.objects.create(id=next(int_iterator), name=test_bad_word, category=self.test_category)
        response = self._request(search=test_search_query)

        self.assertEqual(len(response.data["items"]), 0)
