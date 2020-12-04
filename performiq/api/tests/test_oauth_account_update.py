from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from performiq.api.urls.names import PerformIQPathName
from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase


class OAuthAccountUpdateAPITestCase(ExtendedAPITestCase):

    def _get_url(self, account_id):
        return reverse(Namespace.PERFORMIQ + ":" + PerformIQPathName.OAUTH_ACCOUNTS, kwargs={"pk": account_id})

    def test_oauth_account_is_updated(self):
        user = self.create_test_user()
        oauth_account = OAuthAccount.objects.create(
            user=user,
            oauth_type=OAuthType.GOOGLE_ADS.value,
            name="name",
            email="email@email.email",
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
