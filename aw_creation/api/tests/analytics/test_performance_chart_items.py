from datetime import date
from datetime import datetime
from datetime import timedelta
from itertools import product
from unittest.mock import patch

from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.analytics_charts import ALL_DIMENSIONS
from aw_reporting.analytics_charts import Dimension
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CityStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import RemarkList
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher
from utils.utittests.generic_test import generic_test
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.reverse import reverse


class PerformanceChartItemsAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id, dimension):
        return reverse(Name.Analytics.PERFORMANCE_CHART_ITEMS, [RootNamespace.AW_CREATION, Namespace.ANALYTICS],
                       args=(account_creation_id, dimension))

    def _hide_demo_data(self, user):
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )

    @staticmethod
    def create_stats(account):
        campaign1 = Campaign.objects.create(id=1, name="#1", account=account)
        ad_group1 = AdGroup.objects.create(id=1, name="", campaign=campaign1)
        campaign2 = Campaign.objects.create(id=2, name="#2", account=account)
        ad_group2 = AdGroup.objects.create(id=2, name="", campaign=campaign2)
        date = datetime.now().date() - timedelta(days=1)
        base_stats = dict(date=date, impressions=100, video_views=10, cost=1)
        topic, _ = Topic.objects.get_or_create(id=1, defaults=dict(name="boo"))
        audience, _ = Audience.objects.get_or_create(id=1,
                                                     defaults=dict(name="boo",
                                                                   type="A"))
        remark_list = RemarkList.objects.create(name="Test remark")
        creative, _ = VideoCreative.objects.get_or_create(id=1)
        city, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(name="Babruysk"))
        ad = Ad.objects.create(id=1, ad_group=ad_group1)
        AdStatistic.objects.create(ad=ad, average_position=1, **base_stats)

        for ad_group in (ad_group1, ad_group2):
            stats = dict(ad_group=ad_group, **base_stats)
            AdGroupStatistic.objects.create(average_position=1, **stats)
            GenderStatistic.objects.create(**stats)
            AgeRangeStatistic.objects.create(**stats)
            TopicStatistic.objects.create(topic=topic, **stats)
            AudienceStatistic.objects.create(audience=audience, **stats)
            VideoCreativeStatistic.objects.create(creative=creative, **stats)
            YTChannelStatistic.objects.create(yt_id="123", **stats)
            YTVideoStatistic.objects.create(yt_id="123", **stats)
            KeywordStatistic.objects.create(keyword="123", **stats)
            CityStatistic.objects.create(city=city, **stats)
            RemarkStatistic.objects.create(remark=remark_list, **stats)

    def test_success_get_filter_dates(self):
        user = self.create_test_user()
        self._hide_demo_data(user)

        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        url = self._get_url(account_creation.id, Dimension.ADS)

        today = datetime.now().date()
        response = self.client.post(
            url,
            dict(
                start_date=str(today - timedelta(days=2)),
                end_date=str(today - timedelta(days=1)),
                indicator="impressions",
            )
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            {'items', 'summary'}
        )
        self.assertEqual(len(data['items']), 1)
        self.assertEqual(
            set(data['items'][0].keys()),
            {
                'name',
                'video_view_rate',
                'conversions',
                'ctr',
                'status',
                'view_through',
                'all_conversions',
                'average_cpv',
                'video100rate',
                'video_views',
                'video50rate',
                'clicks',
                'average_position',
                'impressions',
                'video75rate',
                'cost',
                'video25rate',
                'average_cpm',
                'ctr_v',
                "video_clicks",
            }
        )

    def test_success_get_video(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_approved=True,
                                                          account=account)
        self.create_stats(account)
        url = self._get_url(account_creation.id, Dimension.VIDEO)

        with patch("aw_reporting.analytics_charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data
        self.assertEqual(
            set(data.keys()),
            {"items", "summary"}
        )
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(
            set(data["items"][0].keys()),
            {
                "id",
                "name",
                "thumbnail",
                "duration",
                "video_view_rate",
                "conversions",
                "ctr",
                "view_through",
                "all_conversions",
                "average_cpv",
                "video100rate",
                "video_views",
                "video50rate",
                "clicks",
                "impressions",
                "video75rate",
                "cost",
                "video25rate",
                "average_cpm",
                "ctr_v",
                "video_clicks",
            }
        )

    def test_success_demo(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID, Dimension.ADS)

        today = datetime.now().date()
        response = self.client.post(
            url,
            dict(
                start_date=str(today - timedelta(days=2)),
                end_date=str(today - timedelta(days=1)),
                indicator="impressions",
            )
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            {'items', 'summary'}
        )
        self.assertEqual(len(data['items']), 10)
        self.assertEqual(
            set(data['items'][0].keys()),
            {
                'name',
                'video_view_rate',
                'conversions',
                'ctr',
                'status',
                'view_through',
                'all_conversions',
                'average_cpv',
                'video100rate',
                'video_views',
                'video50rate',
                'clicks',
                'average_position',
                'impressions',
                'video75rate',
                'cost',
                'video25rate',
                'average_cpm',
                'ctr_v',
                "clicks_end_cap",
                "clicks_website",
                "clicks_app_store",
                "clicks_cards",
                "clicks_call_to_action_overlay",
            }
        )

    def test_success_get_demo_video(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID, Dimension.VIDEO)

        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data
        self.assertEqual(
            set(data.keys()),
            {'items', 'summary'}
        )
        self.assertGreater(len(data['items']), 1)
        self.assertEqual(
            set(data['items'][0].keys()),
            {
                'id',
                'name',
                'thumbnail',
                'duration',
                'video_view_rate',
                'conversions',
                'ctr',
                'view_through',
                'all_conversions',
                'average_cpv',
                'video100rate',
                'video_views',
                'video50rate',
                'clicks',
                'impressions',
                'video75rate',
                'cost',
                'video25rate',
                'average_cpm',
                'ctr_v',
            }
        )

    def test_success_get_filter_items(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        url = self._get_url(account_creation.id, Dimension.ADS)
        today = datetime.now().date()
        start_date = str(today - timedelta(days=2))
        end_date = str(today - timedelta(days=1))
        response = self.client.post(
            url,
            dict(start_date=start_date,
                 end_date=end_date,
                 campaigns=[1])
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)

        response = self.client.post(
            url,
            dict(start_date=start_date,
                 end_date=end_date,
                 campaigns=[2])
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 0)

    def test_get_all_dimensions(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        with patch("aw_reporting.analytics_charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            for dimension in ALL_DIMENSIONS:
                with self.subTest(dimension):
                    url = self._get_url(account_creation.id, dimension)
                    response = self.client.post(url)
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    self.assertGreater(len(response.data), 1)

    def test_success_get_view_rate_calculation(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group_cpv = AdGroup.objects.create(id=1, name="", campaign=campaign)
        ad_group_cpm = AdGroup.objects.create(id=2, name="", campaign=campaign)
        now = datetime.now()
        for views, ad_group in enumerate((ad_group_cpm, ad_group_cpv)):
            AdGroupStatistic.objects.create(
                date=now,
                ad_group=ad_group,
                average_position=1,
                impressions=10,
                video_views=views,
            )

        url = self._get_url(account_creation.id, Dimension.DEVICE)

        response = self.client.post(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        item = data['items'][0]
        self.assertEqual(item['video_view_rate'], 10)  # 10 %

    @generic_test([
        ("Hide dashboard costs = {}. Dimension = {}".format(hide_costs, dimension), (hide_costs, dimension), dict())
        for hide_costs, dimension in product((True, False), ALL_DIMENSIONS)
    ])
    def test_all_dimensions_hide_costs_independent(self, hide_dashboard_costs, dimension):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: hide_dashboard_costs
        }
        with patch("aw_reporting.analytics_charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            url = self._get_url(account_creation.id, dimension)
            response = self.client.post(url, dict())
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            self.assertGreater(len(items), 0)
            for item in items:
                self.assertIsNotNone(item["cost"])
                self.assertIsNotNone(item["average_cpm"])
                self.assertIsNotNone(item["average_cpv"])

    def test_ads_cost(self):
        any_date_1 = date(2018, 1, 1)
        any_date_2 = any_date_1 + timedelta(days=1)
        views = 123, 234
        costs = 12, 23

        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        ad = Ad.objects.create(ad_group=ad_group)
        AdStatistic.objects.create(average_position=1,
                                   ad=ad,
                                   date=any_date_1,
                                   video_views=views[0],
                                   cost=costs[0])
        AdStatistic.objects.create(average_position=1,
                                   ad=ad,
                                   date=any_date_2,
                                   video_views=views[1],
                                   cost=costs[1])
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        test_cases = (
            ("total", any_date_1, any_date_2, sum(costs)),
            ("filter by date", any_date_1, any_date_1, costs[0]),
        )

        url = self._get_url(account_creation.id, Dimension.ADS)
        with patch("aw_reporting.analytics_charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            for msg, start, end, expected_cost in test_cases:
                with self.subTest(msg=msg):
                    response = self.client.post(url, dict(start_date=start,
                                                          end_date=end,
                                                          is_chf=1))
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    items = response.data["items"]
                    self.assertEqual(len(items), 1)
                    item = items[0]
                    self.assertAlmostEqual(item["cost"], expected_cost)

    def test_ages_cost(self):
        any_date_1 = date(2018, 1, 1)
        any_date_2 = any_date_1 + timedelta(days=1)
        views = 123, 234
        costs = 12, 23

        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        AgeRangeStatistic.objects.create(ad_group=ad_group,
                                         date=any_date_1,
                                         video_views=views[0],
                                         cost=costs[0])
        AgeRangeStatistic.objects.create(ad_group=ad_group,
                                         date=any_date_2,
                                         video_views=views[1],
                                         cost=costs[1])
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        test_cases = (
            ("total", any_date_1, any_date_2, sum(costs)),
            ("filter by date", any_date_1, any_date_1, costs[0]),
        )

        url = self._get_url(account_creation.id, Dimension.AGE)
        with patch("aw_reporting.analytics_charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            for msg, start, end, expected_cost in test_cases:
                with self.subTest(msg=msg):
                    response = self.client.post(url, dict(start_date=start,
                                                          end_date=end,
                                                          is_chf=1))
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    items = response.data["items"]
                    self.assertEqual(len(items), 1)
                    item = items[0]
                    self.assertAlmostEqual(item["cost"], expected_cost)

    def test_device_cost(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)

        today = date(2018, 1, 1)
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        self.assertGreater(today, yesterday)
        self.assertGreater(tomorrow, today)

        expected_cost = 123
        campaign = Campaign.objects.create(id=next(int_iterator),
                                           account=account,
                                           cost=expected_cost)
        ad_group = AdGroup.objects.create(id=next(int_iterator),
                                          campaign=campaign)
        AdGroupStatistic.objects.create(ad_group=ad_group,
                                        date=yesterday,
                                        average_position=1,
                                        impressions=12,
                                        video_views=23,
                                        cost=expected_cost)

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        url = self._get_url(account_creation.id, Dimension.DEVICE)
        with patch("aw_reporting.analytics_charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings), \
             patch_now(today):
            response = self.client.post(url, dict(is_chf=1))
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            self.assertEqual(len(items), 1)
            item = items[0]
            self.assertAlmostEqual(item["cost"], expected_cost)

    def test_ads_average_rate(self):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)

        today = date(2018, 1, 1)
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        self.assertGreater(today, yesterday)
        self.assertGreater(tomorrow, today)

        campaign = Campaign.objects.create(id=next(int_iterator),
                                           account=account,
                                           video_views=123)
        ad_group = AdGroup.objects.create(id=next(int_iterator),
                                          campaign=campaign)
        ad = Ad.objects.create(id=next(int_iterator),
                               ad_group=ad_group)
        views, impressions, cost = 123, 234, 132
        AdStatistic.objects.create(ad=ad,
                                   average_position=1,
                                   date=yesterday,
                                   video_views=views,
                                   impressions=impressions,
                                   cost=cost)
        average_cpm = cost / impressions * 1000
        average_cpv = cost / views
        self.assertNotAlmostEqual(average_cpm, average_cpv)

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }

        url = self._get_url(account_creation.id, Dimension.ADS)
        with patch("aw_reporting.analytics_charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings), \
             patch_now(today):
            response = self.client.post(url, dict(is_chf=1))
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            self.assertEqual(len(items), 1)
            item = items[0]
            self.assertAlmostEqual(item["average_cpm"], average_cpm)
            self.assertAlmostEqual(item["average_cpv"], average_cpv)

    @generic_test([
        ("Show conversions = {}, dimension = {}".format(*args), args, dict())
        for args in product((True, False), ALL_DIMENSIONS)
    ])
    def test_convention_independent(self, show_conversions, dimension):
        user = self.create_test_user()
        self._hide_demo_data(user)
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        self.create_stats(account)
        account_creation = AccountCreation.objects.create(id=next(int_iterator), owner=user, account=account,
                                                          is_approved=True)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.SHOW_CONVERSIONS: show_conversions,
        }
        url = self._get_url(account_creation.id, dimension)
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, dict())
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            self.assertGreater(len(items), 0)
            for item in items:
                self.assertIsNotNone(item["conversions"])
                self.assertIsNotNone(item["all_conversions"])
                self.assertIsNotNone(item["view_through"])
