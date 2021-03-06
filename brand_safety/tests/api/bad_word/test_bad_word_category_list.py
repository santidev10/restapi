from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWordCategory
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class BadWordCategoriesListTestCase(ExtendedAPITestCase):
    def _request(self, query_params=None):
        url = reverse(
            PathNames.BadWord.CATEGORY_LIST,
            [Namespace.BRAND_SAFETY],
        )
        if query_params:
            url += f"?{query_params}"
        return self.client.get(url)

    def setUp(self):
        categories = ["Violence", "Terrorism", "Profanity"]
        for category in categories:
            BadWordCategory.objects.create(name=category)

    def test_not_auth(self):
        response = self._request()
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_no_permissions(self):
        self.create_test_user()
        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_categories_list(self):
        self.create_admin_user()
        response = self._request()
        all_items = BadWordCategory.objects.all()
        self.assertEqual(set(all_items.values_list("id", flat=True)),
                         {item["id"] for item in response.data})
        self.assertEqual(set(all_items.values_list("name", flat=True)),
                         {item["name"] for item in response.data})
        scores_response = self._request(query_params="scoring_options=True")
        self.assertEqual(set(all_items.values_list("id", flat=True)),
                         {item["id"] for item in scores_response.data["categories"]})
        self.assertEqual(set(all_items.values_list("name", flat=True)),
                         {item["name"] for item in scores_response.data["categories"]})

    def test_list_non_admin_success_empty(self):
        self.create_test_user()
        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(len(response.data) == 0)
