from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase


class TrackFiltersAPITestCase(AwReportingAPITestCase):

    def test_success_get(self):
        user = self.create_test_user()
        account = self.create_account(user)
        for i in range(1, 3):
            Campaign.objects.create(
                id=i, name="", account=account, impressions=1)

        url = reverse("aw_reporting_urls:track_filters")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertEqual(
            set(response.data.keys()),
            {
                'accounts',
                'dimension',
                'indicator',
                'breakdown',
            }
        )
        self.assertEqual(len(response.data['accounts']), 1)
        account_data = response.data['accounts'][0]
        self.assertEqual(
            set(account_data.keys()),
            {
                'id',
                'name',
                'start_date',
                'end_date',
                'campaigns',
            }
        )
        self.assertEqual(account_data['id'], account.id)
        self.assertEqual(len(account_data['campaigns']), 2)
        self.assertEqual(
            set(account_data['campaigns'][0].keys()),
            {
                'id',
                'name',
                'start_date',
                'end_date',
            }
        )
