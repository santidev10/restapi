from datetime import timedelta

from django.utils import timezone

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignHistory
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

    def _create_mock_data(self):
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
        return campaign_1, campaign_2, campaign_3

    def test_success(self):
        self.create_test_user()
        campaign_1, campaign_2, campaign_3 = self._create_mock_data()
        h1 = CampaignHistory.objects.create(campaign=campaign_1, changes=dict(budget=1))
        h2 = CampaignHistory.objects.create(campaign=campaign_2, changes=dict(budget=2))
        h3 = CampaignHistory.objects.create(campaign=campaign_3, changes=dict(budget=3))

        # mcc account pk is 1
        response = self.client.get(self._get_url(1))
        data = response.data
        # account pk is 2
        budget_data = data["updated_campaign_budgets"][2]
        self.assertEqual(len(data["budget_history_ids"]), 3)
        self.assertEqual(len(budget_data.values()), 3)
        self.assertEqual({h1.id, h2.id, h3.id}, set(data["budget_history_ids"]))
        self.assertEqual(budget_data[1], h1.budget)
        self.assertEqual(budget_data[2], h2.budget)
        self.assertEqual(budget_data[3], h3.budget)

    def test_should_not_get_synced(self):
        """ Response should not return already synced data """
        now = timezone.now()
        user = self.create_test_user()
        campaign_1, campaign_2, campaign_3 = self._create_mock_data()

        h1 = CampaignHistory.objects.create(owner=user, campaign=campaign_1, changes=dict(budget=1), sync_at=now)
        h2 = CampaignHistory.objects.create(owner=user, campaign=campaign_2, changes=dict(budget=2), sync_at=now)
        h3 = CampaignHistory.objects.create(owner=user, campaign=campaign_3, changes=dict(budget=3))
        # mcc account pk is 1
        response = self.client.get(self._get_url(1))
        data = response.data
        budget_history_ids = data["budget_history_ids"]
        self.assertTrue(h1.id not in budget_history_ids)
        self.assertTrue(h2.id not in budget_history_ids)
        self.assertTrue(h3.id in budget_history_ids)
        self.assertEqual(h3.changes["budget"], data["updated_campaign_budgets"][h3.campaign.account.id][h3.campaign.id])

    def test_should_retrieved_latest_change(self):
        """ Response should return latest changes for budgets with multiple change histories """
        user = self.create_test_user()
        campaign_1, campaign_2, campaign_3 = self._create_mock_data()

        CampaignHistory.objects.create(owner=user, campaign=campaign_1, changes=dict(budget=1))
        CampaignHistory.objects.create(owner=user, campaign=campaign_1, changes=dict(budget=1), sync_at=timezone.now())
        h1 = CampaignHistory.objects.create(owner=user, campaign=campaign_1, changes=dict(budget=1))

        CampaignHistory.objects.create(owner=user, campaign=campaign_2, changes=dict(budget=2), sync_at=timezone.now())
        CampaignHistory.objects.create(owner=user, campaign=campaign_2, changes=dict(budget=2))
        h2 = CampaignHistory.objects.create(owner=user, campaign=campaign_2, changes=dict(budget=2))

        CampaignHistory.objects.create(owner=user, campaign=campaign_3, changes=dict(budget=3))
        CampaignHistory.objects.create(owner=user, campaign=campaign_3, changes=dict(budget=3))
        h3 = CampaignHistory.objects.create(owner=user, campaign=campaign_3, changes=dict(budget=3))

        # mcc account pk is 1
        response = self.client.get(self._get_url(1))
        data = response.data
        budget_history_ids = data["budget_history_ids"]
        # account pk is 2
        budget_data = data["updated_campaign_budgets"][2]
        self.assertEqual(len(data["budget_history_ids"]), 3)
        self.assertEqual(len(budget_data.values()), 3)
        self.assertEqual({h1.id, h2.id, h3.id}, set(budget_history_ids))
        self.assertEqual(budget_data[1], h1.changes["budget"])
        self.assertEqual(budget_data[2], h2.changes["budget"])
        self.assertEqual(budget_data[3], h3.changes["budget"])
