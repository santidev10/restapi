import json
from datetime import datetime, timedelta, date
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.charts import Dimension, ALL_DIMENSIONS
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic, \
    GenderStatistic, AgeRangeStatistic, \
    AudienceStatistic, VideoCreativeStatistic, YTVideoStatistic, \
    YTChannelStatistic, TopicStatistic, \
    KeywordStatistic, CityStatistic, AdStatistic, VideoCreative, GeoTarget, \
    Audience, Topic, Ad, \
    AWConnectionToUserRelation, AWConnection, RemarkStatistic, RemarkList, \
    Opportunity, OpPlacement, SalesForceGoalType
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase, patch_now, int_iterator
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountNamesAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id, dimension):
        return reverse(
            Namespace.AW_CREATION + ":" + Name.Dashboard.CHART_ITEMS,
            args=(account_creation_id, dimension))

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
        city, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(
            name="bobruisk"))
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
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )

        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        url = self._get_url(account_creation.id, Dimension.AD_GROUPS)

        today = datetime.now().date()
        response = self.client.post(
            url,
            json.dumps(dict(
                start_date=str(today - timedelta(days=2)),
                end_date=str(today - timedelta(days=1)),
                indicator="impressions",
            )),
            content_type='application/json',
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
                "video_clicks"
            }
        )

    def test_success_get_video(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_approved=True,
                                                          account=account)
        self.create_stats(account)
        url = self._get_url(account_creation.id, Dimension.VIDEO)

        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
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
                "video_clicks"
            }
        )

    def test_success_demo(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID, Dimension.AD_GROUPS)

        today = datetime.now().date()
        response = self.client.post(
            url,
            json.dumps(dict(
                start_date=str(today - timedelta(days=2)),
                end_date=str(today - timedelta(days=1)),
                indicator="impressions",
            )),
            content_type='application/json',
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
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        url = self._get_url(account_creation.id, Dimension.AD_GROUPS)
        today = datetime.now().date()
        start_date = str(today - timedelta(days=2))
        end_date = str(today - timedelta(days=1))
        response = self.client.post(
            url,
            json.dumps(dict(start_date=start_date,
                            end_date=end_date,
                            campaigns=[1])),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)

        response = self.client.post(
            url,
            json.dumps(dict(start_date=start_date,
                            end_date=end_date,
                            campaigns=[2])),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 0)

    def test_get_all_dimensions(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            for dimension in ALL_DIMENSIONS:
                with self.subTest(dimension):
                    url = self._get_url(account_creation.id, dimension)
                    response = self.client.post(url)
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    self.assertGreater(len(response.data), 1)

    def test_success_get_view_rate_calculation(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group_cpv = AdGroup.objects.create(id=1, name="", campaign=campaign)
        ad_group_cpm = AdGroup.objects.create(id=2, name="", campaign=campaign)
        date = datetime.now()
        for views, ad_group in enumerate((ad_group_cpm, ad_group_cpv)):
            AdGroupStatistic.objects.create(
                date=date,
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

    def test_success_demo_data(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user)
        url = self._get_url(account_creation.id, Dimension.AD_GROUPS)

        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(url)
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
            }
        )

    def test_success_regardless_global_account_visibility(self):
        user = self.create_test_user()
        user.is_staff = True
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )

        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        url = self._get_url(account_creation.id, Dimension.TOPIC)

        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(
                url,
                json.dumps(dict(is_chf=1)),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_hide_costs(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self.create_stats(account)

        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        for dimension in ALL_DIMENSIONS:
            with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher), \
                 self.patch_user_settings(**user_settings), \
                 self.subTest(msg=dimension):
                url = self._get_url(account_creation.id, dimension)
                response = self.client.post(url, dict())
                self.assertEqual(response.status_code, HTTP_200_OK)
                items = response.data["items"]
                self.assertGreater(len(items), 0)
                for item in items:
                    self.assertIsNone(item["cost"])
                    self.assertIsNone(item["average_cpm"])
                    self.assertIsNone(item["average_cpv"])

    def test_ad_groups_client_cost(self):
        any_date_1 = date(2018, 1, 1)
        any_date_2 = any_date_1 + timedelta(days=1)
        views = 123, 234
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=.12)

        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        campaign = Campaign.objects.create(salesforce_placement=placement,
                                           account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        ad = Ad.objects.create(ad_group=ad_group)
        AdStatistic.objects.create(average_position=1,
                                   ad=ad,
                                   date=any_date_1,
                                   video_views=views[0])
        AdStatistic.objects.create(average_position=1,
                                   ad=ad,
                                   date=any_date_2,
                                   video_views=views[1])
        rate = placement.ordered_rate
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False
        }
        test_cases = (
            ("total", any_date_1, any_date_2, sum(views) * rate),
            ("filter by date", any_date_1, any_date_1, views[0] * rate),
        )

        url = self._get_url(account_creation.id, Dimension.AD_GROUPS)
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            for msg, start, end, expected_cost in test_cases:
                with self.subTest(msg=msg):
                    response = self.client.post(url, dict(start_date=start,
                                                          end_date=end))
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    items = response.data["items"]
                    self.assertEqual(len(items), 1)
                    item = items[0]
                    self.assertAlmostEqual(item["cost"], expected_cost)

    def test_ages_client_cost(self):
        any_date_1 = date(2018, 1, 1)
        any_date_2 = any_date_1 + timedelta(days=1)
        views = 123, 234
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(
            id=1, opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=.12)

        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        campaign = Campaign.objects.create(salesforce_placement=placement,
                                           account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        AgeRangeStatistic.objects.create(ad_group=ad_group,
                                         date=any_date_1,
                                         video_views=views[0])
        AgeRangeStatistic.objects.create(ad_group=ad_group,
                                         date=any_date_2,
                                         video_views=views[1])
        rate = placement.ordered_rate
        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False
        }
        test_cases = (
            ("total", any_date_1, any_date_2, sum(views) * rate),
            ("filter by date", any_date_1, any_date_1, views[0] * rate),
        )

        url = self._get_url(account_creation.id, Dimension.AGE)
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            for msg, start, end, expected_cost in test_cases:
                with self.subTest(msg=msg):
                    response = self.client.post(url, dict(start_date=start,
                                                          end_date=end))
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    items = response.data["items"]
                    self.assertEqual(len(items), 1)
                    item = items[0]
                    self.assertAlmostEqual(item["cost"], expected_cost)

    def test_ad_group_statistic_client_cost(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)

        today = date(2018, 1, 1)
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        self.assertGreater(today, yesterday)
        self.assertGreater(tomorrow, today)

        opportunity = Opportunity.objects.create()
        placement_cpv = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=.12)
        placement_cpm = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            ordered_rate=1.2)
        placement_outgoing_fee = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            placement_type=OpPlacement.OUTGOING_FEE_TYPE,
            ordered_rate=123)
        placement_hard_cost_include = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            total_cost=234)
        placement_hard_cost_exclude = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            total_cost=345)
        placement_rate_and_tech_fee_cpv = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=13)
        placement_rate_and_tech_fee_cpm = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=14)
        placement_budget = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET)

        stats_data = [
            (placement_outgoing_fee, dict(),
             dict(impressions=1, video_views=1), True),
            (placement_cpm, dict(), dict(impressions=234), False),
            (placement_cpv, dict(), dict(video_views=123), False),
            (placement_hard_cost_include,
             dict(start_date=yesterday, end_date=today + timedelta(days=3)),
             dict(), False),
            (placement_hard_cost_exclude, dict(start_date=tomorrow), dict(),
             True),
            (placement_rate_and_tech_fee_cpv, dict(),
             dict(cost=56, video_views=341), False),
            (placement_rate_and_tech_fee_cpm, dict(),
             dict(cost=45, impressions=431), False),
            (placement_budget, dict(), dict(cost=432), False),
        ]
        expected_cost = 0
        for placement, camp_data, stats, is_zero in stats_data:
            campaign = Campaign.objects.create(id=next(int_iterator),
                                               account=account,
                                               salesforce_placement=placement,
                                               **camp_data)
            ad_group = AdGroup.objects.create(id=next(int_iterator),
                                              campaign=campaign)
            AdGroupStatistic.objects.create(ad_group=ad_group,
                                            date=yesterday,
                                            average_position=1,
                                            **stats)
            client_cost_kwargs = dict(
                goal_type_id=placement.goal_type_id,
                dynamic_placement=placement.dynamic_placement,
                placement_type=placement.placement_type,
                ordered_rate=placement.ordered_rate,
                impressions=stats.get("impressions") or 0,
                video_views=stats.get("video_views") or 0,
                aw_cost=stats.get("cost") or 0,
                total_cost=placement.total_cost,
                tech_fee=placement.tech_fee,
                start=camp_data.get("start_date"),
                end=camp_data.get("end_date")
            )
            with patch_now(today):
                client_cost = get_client_cost(**client_cost_kwargs)
            if not is_zero:
                self.assertGreater(client_cost, 0,
                                   "Test does not assert case "
                                   "" + str(client_cost_kwargs))
            expected_cost += client_cost

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False
        }

        url = self._get_url(account_creation.id, Dimension.DEVICE)
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings), \
             patch_now(today):
            response = self.client.post(url, dict())
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            self.assertEqual(len(items), 1)
            item = items[0]
            self.assertAlmostEqual(item["cost"], expected_cost)

    def test_ad_statistic_client_cost(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)

        today = date(2018, 1, 1)
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        self.assertGreater(today, yesterday)
        self.assertGreater(tomorrow, today)

        opportunity = Opportunity.objects.create()
        placement_cpv = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=.12)
        placement_cpm = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            ordered_rate=1.2)
        placement_outgoing_fee = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            placement_type=OpPlacement.OUTGOING_FEE_TYPE,
            ordered_rate=123)
        placement_hard_cost_include = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            total_cost=234)
        placement_hard_cost_exclude = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            total_cost=345)
        placement_rate_and_tech_fee_cpv = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=13)
        placement_rate_and_tech_fee_cpm = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=14)
        placement_budget = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET)

        stats_data = [
            (placement_outgoing_fee, dict(),
             dict(impressions=1, video_views=1), True),
            (placement_cpm, dict(), dict(impressions=234), False),
            (placement_cpv, dict(), dict(video_views=123), False),
            (placement_hard_cost_include,
             dict(start_date=yesterday, end_date=today + timedelta(days=3)),
             dict(), False),
            (placement_hard_cost_exclude, dict(start_date=tomorrow), dict(),
             True),
            (placement_rate_and_tech_fee_cpv, dict(),
             dict(cost=56, video_views=341), False),
            (placement_rate_and_tech_fee_cpm, dict(),
             dict(cost=45, impressions=431), False),
            (placement_budget, dict(), dict(cost=432), False),
        ]
        expected_cost = {}
        for placement, camp_data, stats, is_zero in stats_data:
            campaign = Campaign.objects.create(id=next(int_iterator),
                                               account=account,
                                               salesforce_placement=placement,
                                               **camp_data)
            ad_group = AdGroup.objects.create(id=next(int_iterator),
                                              campaign=campaign)
            ad_id = next(int_iterator)
            name = str(ad_id)
            ad = Ad.objects.create(id=ad_id,
                                   creative_name=name,
                                   ad_group=ad_group)
            AdStatistic.objects.create(ad=ad,
                                       average_position=1,
                                       date=yesterday,
                                       **stats)
            client_cost_kwargs = dict(
                goal_type_id=placement.goal_type_id,
                dynamic_placement=placement.dynamic_placement,
                placement_type=placement.placement_type,
                ordered_rate=placement.ordered_rate,
                impressions=stats.get("impressions") or 0,
                video_views=stats.get("video_views") or 0,
                aw_cost=stats.get("cost") or 0,
                total_cost=placement.total_cost,
                tech_fee=placement.tech_fee,
                start=camp_data.get("start_date"),
                end=camp_data.get("end_date")
            )
            with patch_now(today):
                client_cost = get_client_cost(**client_cost_kwargs)
            if not is_zero:
                self.assertGreater(client_cost, 0,
                                   "Test does not assert case "
                                   "" + str(client_cost_kwargs))
            expected_cost["{} #{}".format(name, ad_id)] = client_cost

        user_settings = {
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False
        }

        url = self._get_url(account_creation.id, Dimension.AD_GROUPS)
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings), \
             patch_now(today):
            response = self.client.post(url, dict())
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            self.assertEqual(len(items), OpPlacement.objects.all().count())
            for item in items:
                self.assertAlmostEqual(item["cost"],
                                       expected_cost[item["name"]])
