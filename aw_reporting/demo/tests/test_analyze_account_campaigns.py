from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.test import APITestCase
from aw_reporting.demo.models import *


class AccountNamesAPITestCase(APITestCase):

    def test_success_get(self):
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
