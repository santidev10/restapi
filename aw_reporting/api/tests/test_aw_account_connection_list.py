from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from .base import AwReportingAPITestCase


class AccountConnectionPITestCase(AwReportingAPITestCase):

    def test_success_get(self):
        user = self.create_test_user()
        self.create_account(user)

        url = reverse("aw_reporting_urls:connect_aw_account_list")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(
            set(response.data[0].keys()),
            {
                'email',
                'mcc_accounts',
                'created',
                'update_time',
            }
        )
        self.assertEqual(len(response.data[0]['mcc_accounts']), 1)
        self.assertEqual(
            set(response.data[0]['mcc_accounts'][0].keys()),
            {
                'id',
                'name',
                'currency_code',
                'timezone',
            }
        )



