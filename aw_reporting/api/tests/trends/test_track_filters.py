from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.urls.names import Name
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import Campaign
from saas.urls.namespaces import Namespace


class TrackFiltersAPITestCase(AwReportingAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.Track.FILTERS)

    def test_success_get(self):
        user = self.create_test_user()
        account = self.create_account(user)
        for i in range(1, 3):
            Campaign.objects.create(
                id=i, name="", account=account, impressions=1)

        response = self.client.get(self.url)
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

    def test_demo_account(self):
        recreate_demo_data()
        self.create_test_user()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertEqual(len(response.data['accounts']), 1)
        account_data = response.data['accounts'][0]
        self.assertEqual(account_data["id"], DEMO_ACCOUNT_ID)
