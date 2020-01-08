from django.urls import reverse

from brand_safety.api.urls.names import BrandSafetyPathName
from brand_safety.models import BadWordCategory
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase


class BadWordCategoryListTestCase(ExtendedAPITestCase):
    def _get_url(self):
        return reverse(Namespace.BRAND_SAFETY + ":" + BrandSafetyPathName.BadWord.CATEGORY_LIST)

    def setUp(self):
        categories = ["Violence", "Terrorism", "Profanity"]
        for category in categories:
            BadWordCategory.objects.create(name=category)

    def test_list_admin_success(self):
        self.create_admin_user()
        response = self.client.get(self._get_url())
        self.assertTrue(len(response.data) > 0)

    def test_list_non_admin_success_empty(self):
        self.create_test_user()
        response = self.client.get(self._get_url())
        self.assertTrue(len(response.data) == 0)
