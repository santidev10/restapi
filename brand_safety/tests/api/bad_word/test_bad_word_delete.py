from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.models import AuditLanguage
from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from saas.urls.namespaces import Namespace
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class BadWordDeleteTestCase(ExtendedAPITestCase):
    def _get_url(self, pk=None):
        pk = pk or self.bad_word.id
        return reverse(
            PathNames.BadWord.UPDATE_DELETE,
            [Namespace.BRAND_SAFETY],
            args=(pk,)
        )

    def setUp(self):
        self.category = BadWordCategory.objects.create(name="test category")
        self.bad_word = BadWord.objects.create(
            id=next(int_iterator),
            name="test bad word",
            category=self.category
        )
        self.words = [
            {
                "id": 1,
                "name": "test1",
                "category": BadWordCategory.from_string("profanity"),
                "language": AuditLanguage.from_string("en")
            },
            {
                "id": 2,
                "name": "test2",
                "category": BadWordCategory.from_string("terrorism"),
                "language": AuditLanguage.from_string("en")
            },
            {
                "id": 3,
                "name": "test3",
                "category": BadWordCategory.from_string("terrorism"),
                "language": AuditLanguage.from_string("en")
            },
            {
                "id": 4,
                "name": "test4",
                "category": BadWordCategory.from_string("drugs"),
                "language": AuditLanguage.from_string("sv")
            }
        ]

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

    def test_soft_delete(self):
        self.create_admin_user()
        soft_delete_word = self.words[0]
        BadWord.objects.bulk_create([BadWord(**opts) for opts in self.words])
        response = self._request(soft_delete_word["id"])
        # self.bad_word created during setup + self.words created
        self.assertEqual(len(self.words) + 1, BadWord.all_objects.all().count())
        self.assertTrue(BadWord.all_objects.filter(pk=soft_delete_word["id"]).exists())
