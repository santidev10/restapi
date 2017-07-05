from datetime import datetime, timedelta
from urllib.parse import urlencode
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from .base import AwReportingAPITestCase
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic, \
    CampaignHourlyStatistic


class TrackAccountsDataAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        user = self.create_test_user()
        self.account = self.create_account(user)
        self.campaign = Campaign.objects.create(
            id="1", name="", account=self.account)
        self.ad_group = AdGroup.objects.create(
            id="1", name="", campaign=self.campaign
        )

    def test_success_daily(self):
        today = datetime.now().date()
        test_days = 10
        test_impressions = 100
        for i in range(test_days):
            AdGroupStatistic.objects.create(
                ad_group=self.ad_group,
                average_position=1,
                date=today - timedelta(days=i),
                impressions=test_impressions,
            )

        url = reverse("aw_reporting_urls:track_accounts_data")
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
        )
        url = "{}?{}".format(url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account_data = response.data[0]
        self.assertEqual(
            set(account_data.keys()),
            {
                'id',
                'label',
                'average_1d',
                'average_5d',
                'trend',
            }
        )
        self.assertEqual(len(account_data['trend']), 2)

    def test_success_filter_account(self):
        manager = self.account.managers.first()
        account = Account.objects.create(id=2, name="Name")
        account.managers.add(manager)
        campaign = Campaign.objects.create(id=2, name="", account=account)
        ad_group = AdGroup.objects.create(id=2, name="", campaign=campaign)

        today = datetime.now().date()
        test_days = 10
        test_impressions = 100
        for ag in (self.ad_group, ad_group):
            for i in range(test_days):
                AdGroupStatistic.objects.create(
                    ad_group=ag,
                    average_position=1,
                    date=today - timedelta(days=i),
                    impressions=test_impressions,
                )

        url = reverse("aw_reporting_urls:track_accounts_data")
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
            account=account.id,
        )
        url = "{}?{}".format(url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account_data = response.data[0]
        self.assertEqual(account_data['label'], account.name)

    def test_success_hourly(self):
        today = datetime.now().date()
        test_days = 10
        for i in range(test_days):
            for hour in range(24):
                CampaignHourlyStatistic.objects.create(
                    campaign=self.campaign,
                    date=today - timedelta(days=i),
                    hour=hour,
                    impressions=hour,
                )

        url = reverse("aw_reporting_urls:track_accounts_data")
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
            breakdown="hourly",
        )
        url = "{}?{}".format(url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account = response.data[0]

        self.assertEqual(
            set(account.keys()),
            {
                'id',
                'label',
                'average_1d',
                'average_5d',
                'trend',
            }
        )
        self.assertEqual(len(account['trend']), 2 * 24)
