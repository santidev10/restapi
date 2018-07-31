import json
from datetime import date

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
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
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import int_iterator


class AnalyticsAccountCreationOverviewAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            RootNamespace.AW_CREATION + ":" + Namespace.ANALYTICS + ":" + Name.Analytics.ACCOUNT_OVERVIEW,
            args=(account_creation_id,))

    def _request(self, account_creation_id, status_code=HTTP_200_OK, **kwargs):
        url = self._get_url(account_creation_id)
        response = self.client.post(url, json.dumps(kwargs), content_type="application/json")
        self.assertEqual(response.status_code, status_code)
        return response.data

    def _hide_demo_data(self, user):
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(
                email="me@mail.kz", refresh_token=""), user=user)

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_dashboard")

    def test_cost_is_aw_cost(self):
        self._hide_demo_data(self.user)
        any_date = date(2018, 1, 1)
        another_date = date(2018, 1, 2)
        self.assertNotEqual(any_date, another_date)
        account = Account.objects.create()
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
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        with self.patch_user_settings(**user_settings):
            overview = self._request(account_creation.id, start_date=str(any_date), end_date=str(any_date))
            self.assertEqual(overview["cost"], aw_cost)
