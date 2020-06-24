from datetime import timedelta

from django.utils import timezone

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class PacingReportFlightCampaignAllocationsChangedTestCase(ExtendedAPITestCase):
    @staticmethod
    def _get_url(*args):
        return reverse(Name.PacingReport.FLIGHTS_CAMPAIGN_ALLOCATIONS_CHANGED, [Namespace.AW_REPORTING],
                       args=args)

    def test_success(self):
        self.create_test_user()
        past = timezone.now() - timedelta(10)
        future = timezone.now() + timedelta(10)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            start=past,
            end=future
        )
        flight = Flight.objects.create(id=1, placement=placement, budget=10)

        mcc_account = Account.objects.create(id=1, can_manage_clients=True)

        managed_account = Account.objects.create(id=2)
        managed_account.managers.add(mcc_account)

        campaign_1_goal_allocation = 30
        campaign_2_goal_allocation = 70
        campaign_3_goal_allocation = 100

        campaign_1_budget = flight.budget * campaign_1_goal_allocation / 100
        campaign_2_budget = flight.budget * campaign_2_goal_allocation / 100
        campaign_3_budget = flight.budget * campaign_3_goal_allocation / 100

        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement, account=managed_account, goal_allocation=campaign_1_goal_allocation,
            budget=campaign_1_budget, update_time=future, sync_time=past)
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement, account=managed_account, goal_allocation=campaign_2_goal_allocation,
            budget=campaign_2_budget, update_time=future, sync_time=past)
        campaign_3 = Campaign.objects.create(
            id=3, salesforce_placement=placement, account=managed_account, goal_allocation=campaign_3_goal_allocation,
            budget=campaign_3_budget, update_time=future, sync_time=None)

        response = self.client.get(self._get_url(1))

        self.assertEqual(len(response.data[2].values()), 3)
        self.assertEqual(response.data[2][1], campaign_1.budget)
        self.assertEqual(response.data[2][2], campaign_2.budget)
        self.assertEqual(response.data[2][3], campaign_3.budget)
