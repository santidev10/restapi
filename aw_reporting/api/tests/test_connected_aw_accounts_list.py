from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from .base import AwReportingAPITestCase


class ConnectAWAccountListTestCase(AwReportingAPITestCase):
    _url = reverse(Namespace.AW_REPORTING + ":" + Name.AWAccounts.LIST)

    def test_success_get(self):
        user = self.create_test_user()
        self.create_account(user)

        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(
            set(response.data[0].keys()),
            {
                "id",
                "email",
                "mcc_accounts",
                "created",
                "update_time",
            }
        )
        self.assertEqual(len(response.data[0]["mcc_accounts"]), 1)
        self.assertEqual(
            set(response.data[0]["mcc_accounts"][0].keys()),
            {
                "id",
                "name",
                "currency_code",
                "timezone",
            }
        )
