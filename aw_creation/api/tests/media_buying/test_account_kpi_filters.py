from django.http import QueryDict
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.api.views.media_buying.constants import REPORT_CONFIG
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class MediaBuyingAccountKpiFiltersTestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.MediaBuying.ACCOUNT_KPI_FILTERS,
            [RootNamespace.AW_CREATION, Namespace.MEDIA_BUYING],
            args=(account_creation_id,),
        )

    def test_no_permission_fail(self):
        self.create_test_user()
        account = Account.objects.create(id=1, name="")
        query_prams = QueryDict("targeting=all").urlencode()
        url = f"{self._get_url(account.account_creation.id)}?{query_prams}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_not_visible_account(self):
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        query_prams = QueryDict("targeting=all").urlencode()
        url = f"{self._get_url(account.account_creation.id)}?{query_prams}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_get_success(self):
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        query_prams = QueryDict("targeting=all").urlencode()
        url = f"{self._get_url(account.account_creation.id)}?{query_prams}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), set(REPORT_CONFIG["all"]["aggregations"]))
