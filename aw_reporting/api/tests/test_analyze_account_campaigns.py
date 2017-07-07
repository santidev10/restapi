from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_reporting.demo.models import *
from .base import AwReportingAPITestCase


class AccountNamesAPITestCase(AwReportingAPITestCase):

    def test_success_get(self):
        user = self.create_test_user()
        account = self.create_account(user)
        campaigns_count = 3
        ad_groups_count = 2
        for i in range(campaigns_count):
            c = Campaign.objects.create(id=i, name="", account=account)

            for j in range(ad_groups_count):
                AdGroup.objects.create(id="{}{}".format(i, j), name="", campaign=c)

        url = reverse("aw_reporting_urls:analyze_account_campaigns",
                      args=(account.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), campaigns_count)
        campaign = response.data[0]
        self.assertEqual(
            set(campaign.keys()),
            {
                'id',
                'name',
                'start_date',
                'end_date',
                'status',
                'ad_groups',
            }
        )
        self.assertEqual(len(campaign['ad_groups']), ad_groups_count)
        ad_group = campaign['ad_groups'][0]
        self.assertEqual(
            set(ad_group.keys()),
            {
                'id',
                'name',
                'status',
            }
        )

    def test_success_get_demo(self):
        self.create_test_user()
        url = reverse("aw_reporting_urls:analyze_account_campaigns",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), DEMO_CAMPAIGNS_COUNT)
        campaign = response.data[0]
        self.assertEqual(
            set(campaign.keys()),
            {
                'id',
                'name',
                'start_date',
                'end_date',
                'status',
                'ad_groups',
            }
        )
        self.assertEqual(len(campaign['ad_groups']), len(DEMO_AD_GROUPS))
        ad_group = campaign['ad_groups'][0]
        self.assertEqual(
            set(ad_group.keys()),
            {
                'id',
                'name',
                'status',
            }
        )
