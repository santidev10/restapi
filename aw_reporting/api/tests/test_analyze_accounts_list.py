from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from .base import AwReportingAPITestCase
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from aw_reporting.models import Campaign, AdGroup, VideoCreative, VideoCreativeStatistic
from datetime import datetime


class AccountNamesAPITestCase(AwReportingAPITestCase):

    def test_success_get(self):
        user = self.create_test_user()
        account = self.create_account(user)
        Campaign.objects.create(id=1, name="", account=account,
                                impressions=4, video_views=2, cost=1, clicks=1)

        url = reverse("aw_reporting_urls:analyze_accounts_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()),
                         {'items', 'items_count', 'max_page', 'current_page'})

        self.assertEqual(len(response.data['items']), 2)
        account_data = response.data['items'][1]
        self.assertEqual(set(account_data.keys()), self.account_list_header_fields)
        self.assertEqual(account_data['id'], account.id)

    def test_success_get_demo(self):
        self.create_test_user()

        url = reverse("aw_reporting_urls:analyze_accounts_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()),
                         {'items', 'items_count', 'max_page', 'current_page'})

        self.assertEqual(len(response.data['items']), 1)
        account = response.data['items'][0]
        self.assertEqual(set(account.keys()), self.account_list_header_fields)
        self.assertEqual(account['id'], 'demo')
