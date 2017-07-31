from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_creation.models import AccountCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID, DemoAccount
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic
from saas.utils_tests import ExtendedAPITestCase
from datetime import datetime


class AccountNamesAPITestCase(ExtendedAPITestCase):

    def test_success_get(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user, account=account)

        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        start = datetime(2009, 3, 10).date()
        end = datetime(2017, 1, 1).date()
        stats = dict(
            ad_group=ad_group,
            impressions=10,
            video_views=5,
            clicks=1,
            cost=1,
            average_position=1
        )
        AdGroupStatistic.objects.create(date=start, **stats)
        AdGroupStatistic.objects.create(date=end, **stats)

        url = reverse("aw_creation_urls:performance_targeting_filters",
                      args=(account_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'start_date', 'end_date',
                'campaigns',
                'average_cpv', 'ctr', 'video_view_rate', 'ctr_v', 'average_cpm'
            }
        )
        self.assertEqual(len(response.data['campaigns']), 1)
        self.assertEqual(response.data['start_date'], start)
        self.assertEqual(response.data['end_date'], end)

    def test_success_get_demo(self):
        self.create_test_user()
        url = reverse("aw_creation_urls:performance_targeting_filters",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'start_date', 'end_date',
                'campaigns',
                'average_cpv', 'ctr', 'video_view_rate', 'ctr_v', 'average_cpm'
            }
        )
        self.assertEqual(len(response.data['campaigns']), 2)
        account = DemoAccount()
        self.assertEqual(response.data['start_date'], account.start_date)
        self.assertEqual(response.data['end_date'], account.end_date)
