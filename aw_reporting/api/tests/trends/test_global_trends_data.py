from datetime import datetime, timedelta

from django.core.urlresolvers import reverse
from django.utils.http import urlencode
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED

from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.api.urls.names import Name
from aw_reporting.models import Campaign, AdGroup, AdGroupStatistic, \
    CampaignHourlyStatistic, Account
from aw_reporting.settings import InstanceSettingsKey
from saas.urls.namespaces import Namespace
from utils.datetime import now_in_default_tz
from utils.utils_tests import patch_instance_settings


class GlobalTrendsDataTestCase(AwReportingAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.GlobalTrends.DATA)

    def _create_test_data(self):
        user = self.create_test_user()
        account = self.create_account(user)
        campaign = Campaign.objects.create(
            id="1", name="", account=account)
        ad_group = AdGroup.objects.create(
            id="1", name="", campaign=campaign
        )
        return user, account, campaign, ad_group

    def test_authorization_required(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_success_daily(self):
        _, account, _, ad_group = self._create_test_data()
        manager = account.managers.first()
        today = datetime.now().date()
        test_days = 10
        test_impressions = 100
        for i in range(test_days):
            AdGroupStatistic.objects.create(
                ad_group=ad_group,
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
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
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
        _, account, _, ad_group_1 = self._create_test_data()
        manager = account.managers.first()
        account = Account.objects.create(id=2, name="Name")
        account.managers.add(manager)
        campaign = Campaign.objects.create(id=2, name="", account=account)
        ad_group = AdGroup.objects.create(id=2, name="", campaign=campaign)

        today = datetime.now().date()
        test_days = 10
        test_impressions = 100
        for ag in (ad_group_1, ad_group):
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
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account_data = response.data[0]
        self.assertEqual(account_data['label'], account.name)

    def test_success_hourly(self):
        _, account, campaign, _ = self._create_test_data()
        manager = account.managers.first()
        today = datetime.now().date()
        test_days = 10
        for i in range(test_days):
            for hour in range(24):
                CampaignHourlyStatistic.objects.create(
                    campaign=campaign,
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
        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
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

    def _create_ad_group_statistic(self, uid, account):
        yesterday = now_in_default_tz().date() - timedelta(days=1)
        campaign = Campaign.objects.create(id=uid, account=account)
        ad_group = AdGroup.objects.create(id=uid, campaign=campaign)
        AdGroupStatistic.objects.create(date=yesterday, ad_group=ad_group,
                                        video_views=1, average_position=1)

    def test_filter_manage_account(self):
        self.create_test_user()
        manager = Account.objects.create(id="manager")
        account = Account.objects.create(id="account")
        account.managers.add(manager)
        account.save()
        irrelevant_acc = Account.objects.create(id="irrelevant acc")

        self._create_ad_group_statistic(1, account)
        self._create_ad_group_statistic(2, irrelevant_acc)

        instance_settings = {
            InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS: [manager.id]
        }
        with patch_instance_settings(**instance_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account_data = response.data[0]
        self.assertEqual(account_data['label'], account.name)

    # def test_filter_