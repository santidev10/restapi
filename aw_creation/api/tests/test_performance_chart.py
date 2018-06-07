import json
from datetime import datetime, timedelta
from itertools import product
from unittest.mock import patch

from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_reporting.charts import Indicator, ALL_DIMENSIONS
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic, \
    GenderStatistic, AgeRangeStatistic, AudienceStatistic, \
    VideoCreativeStatistic, YTVideoStatistic, YTChannelStatistic, \
    TopicStatistic, KeywordStatistic, CityStatistic, AdStatistic, \
    VideoCreative, GeoTarget, Audience, Topic, Ad, AWConnection, \
    AWConnectionToUserRelation
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountNamesAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Namespace.AW_CREATION + ":" + Name.Dashboard.PERFORMANCE_CHART,
            args=(account_creation_id,))

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

        url = self._get_url(account_creation.id)

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
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['title'], "Summary for 2 campaigns")
        self.assertEqual(data[1]['title'], "#1")
        self.assertEqual(data[2]['title'], "#2")

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

        url = self._get_url(account_creation.id)
        response = self.client.post(
            url,
            json.dumps(dict(campaigns=["1"])),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['title'], "#1")
        self.assertEqual(len(data[0]['data'][0]['trend']), 1)

    def test_all_dimensions(self):
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

        url = self._get_url(account_creation.id)
        filters = {
            'indicator': 'video_view_rate',
        }
        for dimension in ('device', 'gender', 'age', 'topic',
                          'interest', 'creative', 'channel', 'video',
                          'keyword', 'location', 'ad'):
            filters['dimension'] = dimension
            with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                       new=SingleDatabaseApiConnectorPatcher):
                response = self.client.post(
                    url, json.dumps(filters),
                    content_type='application/json',
                )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data), 3)
            self.assertEqual(len(response.data[0]['data']), 1)

    def test_success_get_no_account(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )

        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          sync_at=timezone.now())

        account = Account.objects.create(id=1, name="")
        self.create_stats(account)  # create stats that won't be visible

        url = self._get_url(account_creation.id)

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
        self.assertEqual(len(response.data), 0)

    def test_success_demo(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID)

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
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['title'], "Summary for 2 campaigns")
        self.assertEqual(len(data[0]['data'][0]['trend']), 2)
        self.assertEqual(data[1]['title'], "Campaign #demo1")
        self.assertEqual(data[2]['title'], "Campaign #demo2")

    def test_success_demo_data(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user)
        url = self._get_url(account_creation.id)

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
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['title'], "Summary for 2 campaigns")
        self.assertEqual(len(data[0]['data'][0]['trend']), 2)
        self.assertEqual(data[1]['title'], "Campaign #demo1")
        self.assertEqual(data[2]['title'], "Campaign #demo2")

    def test_hide_cost_based_on_user_settings(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_paused=True)
        self.assertNotEqual(account_creation.status, AccountCreation.STATUS_PENDING)

        dimensions = ALL_DIMENSIONS
        cost_indicators = Indicator.CPM, Indicator.CPV, Indicator.COSTS
        account_ids = account_creation.id, DEMO_ACCOUNT_ID

        test_cases = list(product(dimensions, cost_indicators, account_ids))

        def test_availability(expected_status_code):
            for dimension, indicator, account_id in test_cases:
                url = self._get_url(account_id)
                response = self.client.post(url,
                                            json.dumps(dict(
                                                indicator=indicator,
                                                dimention=dimension
                                            )),
                                            content_type='application/json')
                is_demo = account_id == DEMO_ACCOUNT_ID
                msg = "Demo: {}; Dimension: {}; Indicator: {}".format(
                    is_demo, dimension, indicator)
                self.assertEqual(response.status_code, expected_status_code,
                                 msg)

        # show
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False
        }
        with self.patch_user_settings(**user_settings):
            test_availability(HTTP_200_OK)

        # hide
        user_settings = {
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        with self.patch_user_settings(**user_settings):
            test_availability(HTTP_404_NOT_FOUND)
