from datetime import date

from django.urls import reverse

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.tools.forecast_tool.forecast_tool_estimate import ForecastToolEstimate
from saas.urls.namespaces import Namespace
from userprofile.constants import UserSettingsKey
from userprofile.constants import StaticPermissions
from utils.unittests.test_case import ExtendedAPITestCase


class ForecastToolEstimateAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.url = reverse(Namespace.AW_REPORTING + ":" + Name.ForecastTool.ESTIMATE)

    def test_opportunities_queryset(self):
        cost = 60.0
        impressions = 30
        expected_average_cpm = cost / impressions * 1000 + ForecastToolEstimate.CPM_BUFFER
        self.create_test_user(perms={
            StaticPermissions.FORECAST_TOOL: True,
            StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS: False,
            StaticPermissions.MANAGED_SERVICE__GLOBAL_ACCOUNT_VISIBILITY: False,
        })
        account = Account.objects.create()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        campaign = Campaign.objects.create(salesforce_placement=placement, id="1", name="", account=account)
        ad_group = AdGroup.objects.create(id="1", name="", campaign=campaign)
        AdGroupStatistic.objects.create(
            date=date(2017, 11, 21), ad_group=ad_group, cost=cost, average_position=1, impressions=impressions)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.post(self.url)
        self.assertEqual(response.data.get("average_cpm"), expected_average_cpm)
