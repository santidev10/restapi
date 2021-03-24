from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from oauth.api.urls.names import OAuthPathName
from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


class OAuthAccountUpdateAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_id):
        return reverse(Namespace.OAUTH + ":" + OAuthPathName.OAUTH_ACCOUNT_UPDATE, kwargs={"pk": account_id})

    def test_permission(self):
        self.create_test_user()
        response = self.client.get(self._get_url(0))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_oauth_account_is_updated(self):
        """test that an OAuthAccount is modified successfully"""
        user = self.create_admin_user()
        uniqifier = next(int_iterator)
        oauth_account = OAuthAccount.objects.create(
            user=user,
            oauth_type=OAuthType.GOOGLE_ADS.value,
            name=f"name{uniqifier}",
            email=f"email{uniqifier}@email.email",
            token="token",
            refresh_token="refresh_token",
            is_enabled=True,
        )
        is_enabled = "is_enabled"
        data = {is_enabled: False}
        response = self.client.patch(self._get_url(account_id=oauth_account.id), data=data)
        self.assertEqual(response.status_code, HTTP_200_OK)
        oauth_account.refresh_from_db()
        self.assertEqual(oauth_account.is_enabled, data.get(is_enabled), response.data.get(is_enabled))

    def test_non_owners_forbidden(self):
        """users should not be able to modify OAuthAccounts that do not belong to them"""
        owner = self.create_test_user()
        not_owner = self.create_test_user(email=f"email{next(int_iterator)}@domain.com")
        self.request_user = not_owner
        uniqifier = next(int_iterator)
        oauth_account = OAuthAccount.objects.create(
            user=owner,
            oauth_type=OAuthType.GOOGLE_ADS.value,
            name=f"name{uniqifier}",
            email=f"email{uniqifier}@email.email",
            token="token",
            refresh_token="refresh_token",
            is_enabled=True,
        )
        data = {"is_enabled": False}
        response = self.client.patch(self._get_url(account_id=oauth_account.id), data=data)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
