from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_404_NOT_FOUND, \
    HTTP_401_UNAUTHORIZED

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase


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
        self.create_test_user()
        test_id = "test_id"
        self.assertEqual(Account.objects.filter(id=test_id).count(), 0)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        url = self._get_url(account_id=test_id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_404_if_account_not_in_visible_accounts(self):
        self.create_test_user()
        account = Account.objects.create(id="account_id")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: []
        }

        url = self._get_url(account_id=account.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success_if_account_is_visible(self):
        self.create_test_user()
        account = Account.objects.create(id="account_id")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }

        url = self._get_url(account_id=account.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_if_visible_all_accounts(self):
        self.create_test_user()
        account = Account.objects.create(id="account_id")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.VISIBLE_ACCOUNTS: []
        }

        url = self._get_url(account_id=account.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_get(self):
        user = self.create_test_user()
        account = Account.objects.create(id="account_id",
                                                 skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(id="ac_cr_id",
                                                          account=account,
                                                          owner=user)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        url = self._get_url(account_id=account.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["id"], account_creation.id)
