from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace
from userprofile.constants import UserSettingsKey
from userprofile.constants import StaticPermissions
from utils.unittests.test_case import ExtendedAPITestCase


class AccountCreationByAccountAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_id):
        return reverse(
            Namespace.AW_CREATION + ":" + Name.Dashboard.ACCOUNT_CREATION_BY_ACCOUNT,
            kwargs=dict(account_id=account_id))

    def test_authorization_required(self):
        url = self._get_url("test")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_404_if_account_not_account(self):
        self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: True,
        })
        test_id = 111
        self.assertEqual(Account.objects.filter(id=test_id).count(), 0)
        url = self._get_url(account_id=test_id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_404_if_account_not_in_visible_accounts(self):
        self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: False,
        })
        account = Account.objects.create(id=111)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: []
        }
        url = self._get_url(account_id=account.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success_if_account_is_visible(self):
        self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: False,
        })
        account = Account.objects.create(id=111)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        url = self._get_url(account_id=account.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_if_visible_all_accounts(self):
        self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: True,
        })
        account = Account.objects.create(id=111)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: []
        }

        url = self._get_url(account_id=account.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_get(self):
        user = self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: True,
        })
        account = Account.objects.create(id=111,
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(id=333,
                                                          account=account,
                                                          owner=user)
        url = self._get_url(account_id=account.id)
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
