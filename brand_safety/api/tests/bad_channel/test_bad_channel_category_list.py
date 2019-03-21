from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models.bad_channel import ALL_BAD_CHANNEL_CATEGORIES
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BadChannelCategoriesListTestCase(ExtendedAPITestCase):
    def _request(self):
        url = reverse(
            PathNames.BadChannel.CATEGORY_LIST,
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

    def test_categories_list(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(list(response.data), sorted(ALL_BAD_CHANNEL_CATEGORIES))
