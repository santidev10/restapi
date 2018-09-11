from datetime import datetime
from datetime import timedelta

from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_reporting.dashboard_charts import ALL_DIMENSIONS
from aw_reporting.dashboard_charts import Dimension
from aw_reporting.models import AWConnection, CLICKS_STATS
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
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import generic_test
from utils.utils_tests import int_iterator
from utils.utils_tests import reverse


class PerformanceChartItemsAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id, dimension):
        return reverse(Name.Dashboard.PERFORMANCE_CHART_ITEMS, [RootNamespace.AW_CREATION, Namespace.DASHBOARD],
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

    def test_success_regardless_global_account_visibility(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        self._hide_demo_data(user)

        account = Account.objects.create(id=1, name="")
        self.create_stats(account)

        url = self._get_url(account.account_creation.id, Dimension.TOPIC)

        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, dict())
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_cta_fields_in_topic_dimension_response(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        self._hide_demo_data(user)

        account = Account.objects.create(id=1, name="")
        self.create_stats(account)

        url = self._get_url(account.account_creation.id, Dimension.TOPIC)

        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, dict())
        self.assertEqual(response.status_code, HTTP_200_OK)
        for field in CLICKS_STATS:
            self.assertIn(field, response.data.get("items")[0].keys())

    def test_cta_fields_in_gender_dimension_response(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        self._hide_demo_data(user)

        account = Account.objects.create(id=1, name="")
        self.create_stats(account)

        url = self._get_url(account.account_creation.id, Dimension.GENDER)

        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, dict())
        self.assertEqual(response.status_code, HTTP_200_OK)
        for field in CLICKS_STATS:
            self.assertIn(field, response.data.get("items")[0].keys())

    @generic_test([
        (dimension, (dimension,), dict())
        for dimension in ALL_DIMENSIONS
    ])
    def test_conversions_are_hidden(self, dimension):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        self._hide_demo_data(user)
        account = Account.objects.create(id=next(int_iterator))
        self.create_stats(account)
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        AdGroup.objects.create(id=next(int_iterator), campaign=campaign, conversions=2,
                               all_conversions=3, view_through=4)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.SHOW_CONVERSIONS: False,
        }
        url = self._get_url(account.account_creation.id, dimension)
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, dict())
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            self.assertGreater(len(items), 0)
            for item in items:
                self.assertNotIn("conversions", item)
                self.assertNotIn("all_conversions", item)
                self.assertNotIn("view_through", item)

    @generic_test([
        (dimension, (dimension,), dict())
        for dimension in ALL_DIMENSIONS
    ])
    def test_conversions_are_visible(self, dimension):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        self._hide_demo_data(user)
        account = Account.objects.create(id=next(int_iterator))
        self.create_stats(account)
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        AdGroup.objects.create(id=next(int_iterator), campaign=campaign, conversions=2,
                               all_conversions=3, view_through=4)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.SHOW_CONVERSIONS: True,
        }
        url = self._get_url(account.account_creation.id, dimension)
        with self.patch_user_settings(**user_settings):
            response = self.client.post(url, dict())
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data["items"]
            self.assertGreater(len(items), 0)
            for item in items:
                self.assertIsNotNone(item["conversions"])
                self.assertIsNotNone(item["all_conversions"])
                self.assertIsNotNone(item["view_through"])
