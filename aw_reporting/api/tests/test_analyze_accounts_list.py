from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from .base import AwReportingAPITestCase
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from aw_reporting.models import Campaign, AdGroup, VideoCreative, VideoCreativeStatistic
from datetime import datetime


class AccountNamesAPITestCase(AwReportingAPITestCase):

    detail_fields = {
        'id', 'name', 'account_creation', 'status', 'start', 'end', 'is_optimization_active', 'is_changed',
        'creative_count', 'keywords_count', 'videos_count', 'goal_units', 'channels_count', 'campaigns_count',
        'ad_groups_count', "weekly_chart", 'is_ended', 'is_approved', 'structure', 'bidding_type',
        'video_ad_format', 'delivery_method', 'video_networks', 'goal_type', 'is_paused', 'type',
        'goal_charts', 'creative',
    }

    def test_success_get(self):
        user = self.create_test_user()
        account = self.create_account(user)
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        creative = VideoCreative.objects.create(id="lal")
        VideoCreativeStatistic.objects.create(creative=creative, ad_group=ad_group, date=datetime.now())

        url = reverse("aw_reporting_urls:analyze_accounts_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            with patch("aw_reporting.api.serializers.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher):
                response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()),
                         {'items', 'items_count', 'max_page', 'current_page'})

        self.assertEqual(len(response.data['items']), 2)
        account_data = response.data['items'][1]
        self.assertEqual(set(account_data.keys()), self.detail_fields)
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
        self.assertEqual(set(account.keys()), self.detail_fields)
        self.assertEqual(account['id'], 'demo')
