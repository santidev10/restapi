from datetime import date

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase, generic_test
from utils.utils_tests import int_iterator


class AnalyticsAccountCreationOverviewAPITestCase(ExtendedAPITestCase):
    _keys = {
        "age",
        "all_conversions",
        "average_cpm",
        "average_cpv",
        "average_cpv_bottom",
        "average_cpv_top",
        "clicks",
        "clicks_last_week",
        "clicks_this_week",
        "conversions",
        "cost",
        "cost_last_week",
        "cost_this_week",
        "ctr",
        "ctr_bottom",
        "ctr_top",
        "ctr_v",
        "ctr_v_bottom",
        "ctr_v_top",
        "delivered_cost",
        "delivered_impressions",
        "delivered_video_views",
        "device",
        "gender",
        "has_statistics",
        "impressions",
        "impressions_last_week",
        "impressions_this_week",
        "location",
        "plan_cost",
        "plan_impressions",
        "plan_video_views",
        "video100rate",
        "video25rate",
        "video50rate",
        "video75rate",
        "video_clicks",
        "video_view_rate",
        "video_view_rate_bottom",
        "video_view_rate_top",
        "video_views",
        "video_views_last_week",
        "video_views_this_week",
        "view_through",
    }

    def _get_url(self, account_creation_id):
        return reverse(
            RootNamespace.AW_CREATION + ":" + Namespace.ANALYTICS + ":" + Name.Analytics.ACCOUNT_OVERVIEW,
            args=(account_creation_id,))

    def _request(self, account_creation_id, status_code=HTTP_200_OK, **kwargs):
        url = self._get_url(account_creation_id)
        response = self.client.post(url, kwargs)
        self.assertEqual(response.status_code, status_code)
        return response.data

    def _hide_demo_data(self, user):
        AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""),
            user=user)

    def setUp(self):
        self.user = self.create_test_user()

    def test_success(self):
        account = Account.objects.create(skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        overview = self._request(account_creation.id)
        self.assertEqual(set(overview.keys()), self._keys)

    def test_success_demo(self):
        overview = self._request(DEMO_ACCOUNT_ID)
        self.assertEqual(set(overview.keys()), self._keys)

    def test_cost_is_aw_cost(self):
        self._hide_demo_data(self.user)
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create(skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), account=account, owner=self.request_user,
            is_approved=True)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        aw_cost = 123
        aw_cost_irrelevant = 23
        self.assertGreater(aw_cost_irrelevant, 0)
        placement = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity)
        campaign = Campaign.objects.create(id=next(int_iterator), salesforce_placement=placement, account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, cost=aw_cost, average_position=1)
        AdGroupStatistic.objects.create(date=another_date, ad_group=ad_group, cost=aw_cost_irrelevant,
                                        average_position=1)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account_creation.id, start_date=str(any_date), end_date=str(any_date))
            self.assertEqual(overview["cost"], aw_cost)

    @generic_test([
        ("Show conversions", (True,), dict()),
        ("Hide conversions", (False,), dict()),
    ])
    def test_conversions_are_always_visible(self, show_conversions):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        conversions = 2
        all_conversions = 3
        view_through = 4
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(id=next(int_iterator), owner=user, account=account)
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=any_date, average_position=1,
                                        conversions=conversions,
                                        all_conversions=all_conversions,
                                        view_through=view_through)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.SHOW_CONVERSIONS: show_conversions,
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account_creation.id)
            self.assertEqual(overview["conversions"], conversions)
            self.assertEqual(overview["all_conversions"], all_conversions)
            self.assertEqual(overview["view_through"], view_through)
