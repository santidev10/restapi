from django.http import QueryDict
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class MediaBuyingAccountTargetingTestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.MediaBuying.ACCOUNT_TARGETING,
            [RootNamespace.AW_CREATION, Namespace.MEDIA_BUYING],
            args=(account_creation_id,),
        )

    def test_no_permission_fail(self):
        self.create_test_user()
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self._get_url(account.account_creation.id))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_fail_non_visible_account(self):
        self.create_admin_user()
        account = Account.objects.create()
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: []
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self._get_url(account.account_creation.id))
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_get_success(self):
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            name="", is_managed=False, owner=user,
            account=account, is_approved=True)
        query_prams = QueryDict("targeting=all").urlencode()
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        url = f"{self._get_url(account_creation.id)}?{query_prams}"
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), {"summary", "current_page", "items", "items_count", "max_page"})

    def test_success_no_overall_summary(self):
        """ Overall summary should be None if targeting_status filter is applied """
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="", skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            name="", is_managed=False, owner=user,
            account=account, is_approved=True)
        query_prams = QueryDict("targeting=all&targeting_status=1").urlencode()
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        url = f"{self._get_url(account_creation.id)}?{query_prams}"
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), {"summary", "current_page", "items", "items_count", "max_page"})
        self.assertEqual(data["summary"], None)
