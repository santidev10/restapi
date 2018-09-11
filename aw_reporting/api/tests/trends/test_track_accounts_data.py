from datetime import datetime, timedelta, date
from urllib.parse import urlencode

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.urls.names import Name
from aw_reporting.analytics_charts import Indicator, Breakdown
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic, \
    CampaignHourlyStatistic
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import generic_test, patch_now


class TrackAccountsDataAPITestCase(AwReportingAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.Track.DATA)

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

        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
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

        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
            account=account.id,
        )
        url = "{}?{}".format(self.url, urlencode(filters))
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

        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
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

    @generic_test((
            ("Show AW rates", (True,), {}),
            ("Hide AW rates", (False,), {}),
    ))
    def test_aw_rate_settings_does_not_affect_rates(self, aw_rates):
        """
        Bug: Trends > "Show real (AdWords) costs on the dashboard" affects data on Media Buying -> Trends
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2818
        """
        any_date = date(2018, 1, 1)
        views, cost = 12, 23
        AdGroupStatistic.objects.create(ad_group=self.ad_group, date=any_date, video_views=views, cost=cost,
                                        average_position=1)
        expected_cpv = cost / views
        filters = dict(
            start_date=any_date,
            end_date=any_date,
            indicator=Indicator.CPV,
            breakdown=Breakdown.DAILY
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: aw_rates
        }
        self.assertGreater(expected_cpv, 0)
        with self.patch_user_settings(**user_settings),\
                patch_now(any_date):
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data), 1)
            self.assertEqual(len(response.data[0]["trend"]), 1)
            item = response.data[0]
            self.assertIsNotNone(item["average_1d"])
            self.assertAlmostEqual(item["average_1d"], expected_cpv)
            self.assertAlmostEqual(item["trend"][0]["value"], expected_cpv)

    def test_apex_deal(self):
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

        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
            breakdown="hourly",
            apex_deal="1",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 0)
