import json
from datetime import datetime, timedelta
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_reporting.charts import Dimension, ALL_DIMENSIONS
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic, \
    GenderStatistic, AgeRangeStatistic, \
    AudienceStatistic, VideoCreativeStatistic, YTVideoStatistic, \
    YTChannelStatistic, TopicStatistic, \
    KeywordStatistic, CityStatistic, AdStatistic, VideoCreative, GeoTarget, \
    Audience, Topic, Ad, \
    AWConnectionToUserRelation, AWConnection, RemarkStatistic, RemarkList
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountNamesAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id, dimension):
        return reverse(Namespace.AW_CREATION + ":" + Name.Dashboard.CHART_ITEMS,
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
            {'items', 'summary'}
        )
        self.assertEqual(len(data['items']), 1)
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
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
            for dimension in ALL_DIMENSIONS:
                url = self._get_url(account_creation.id, dimension)
                response = self.client.post(url, dict())
                self.assertEqual(response.status_code, HTTP_200_OK)
                items = response.data["items"]
                self.assertGreater(len(items), 0)
                for item in items:
                    self.assertIsNone(item["cost"])
                    self.assertIsNone(item["average_cpm"])
                    self.assertIsNone(item["average_cpv"])
