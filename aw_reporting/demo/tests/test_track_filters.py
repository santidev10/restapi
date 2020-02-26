from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from utils.unittests.test_case import ExtendedAPITestCase


class TrackFiltersAPITestCase(ExtendedAPITestCase):

    def test_success_get(self):
        recreate_demo_data()
        self.create_test_user()
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
        account = response.data['accounts'][0]
        self.assertEqual(
            set(account.keys()),
            {
                'id',
                'name',
                'start_date',
                'end_date',
                'campaigns',
            }
        )
        self.assertEqual(account['id'], DEMO_ACCOUNT_ID)
        self.assertEqual(len(account['campaigns']), 2)
        self.assertEqual(
            set(account['campaigns'][0].keys()),
            {
                'id',
                'name',
                'start_date',
                'end_date',
            }
        )
