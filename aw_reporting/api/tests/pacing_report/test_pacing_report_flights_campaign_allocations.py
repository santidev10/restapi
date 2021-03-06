import json

from django.urls import reverse
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED

from aw_reporting.api.urls.names import Name
from aw_reporting.api.views.pacing_report.pacing_report_flights_campaign_allocations import \
    PacingReportFlightsCampaignAllocationsView
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignHistory
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.test_case import ExtendedAPITestCase


class PacingReportFlightCampaignAllocationsTestCase(ExtendedAPITestCase):
    min_allocation = PacingReportFlightsCampaignAllocationsView.MIN_ALLOCATION_SUM
    max_allocation = PacingReportFlightsCampaignAllocationsView.MAX_ALLOCATION_SUM

    def _update(self, flight_id, data):
        url = reverse(
            "{namespace}:{viewname}".format(
                namespace=Namespace.AW_REPORTING,
                viewname=Name.PacingReport.FLIGHTS_CAMPAIGN_ALLOCATIONS),
            kwargs=dict(pk=str(flight_id)))
        return self.client.put(url, json.dumps(data),
                               content_type="application/json")

    def test_auth_required(self):
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)

        put_data = dict()
        response = self._update(flight.id, put_data)

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_bad_request_on_empty_campaign_set(self):
        self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)

        put_data = dict()
        response = self._update(flight.id, put_data)

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_bad_request_on_wrong_campaign_set(self):
        self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        Campaign.objects.create(id=1, salesforce_placement=placement)
        irrelevant_campaign = Campaign.objects.create(id=2)

        put_data = {irrelevant_campaign.id: 100}
        response = self._update(flight.id, put_data)

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_bad_request_on_wrong_percentage(self):
        self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement)
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement)

        put_data = {campaign_1.id: 10,
                    campaign_2.id: 10}
        self.assertNotEqual(sum(put_data.values()), 100)

        response = self._update(flight.id, put_data)

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success(self):
        self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        account = Account.objects.create(id=1)

        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement, account=account, status="serving")
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement, account=account, status="serving")

        allocation_1, allocation_2 = 70, 30
        put_data = {
            "flight_budget": 100,
            campaign_1.id: allocation_1,
            campaign_2.id: allocation_2
        }

        response = self._update(flight.id, put_data)
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        campaign_1.refresh_from_db()
        campaign_2.refresh_from_db()
        self.assertEqual(campaign_1.goal_allocation, allocation_1)
        self.assertEqual(campaign_2.goal_allocation, allocation_2)

    def test_success_response(self):
        self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        account = Account.objects.create(id=1)

        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement, account=account, status="serving")
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement, account=account, status="serving")
        campaign_3 = Campaign.objects.create(
            id=3, salesforce_placement=placement, account=account, status="serving")
        campaign_4 = Campaign.objects.create(
            id=4, salesforce_placement=placement, account=account, status="serving")

        allocation_1, allocation_2, allocation_3, allocation_4 = 70, 20, 5, 5
        put_data = {
            "flight_budget": 100,
            campaign_1.id: allocation_1,
            campaign_2.id: allocation_2,
            campaign_3.id: allocation_3,
            campaign_4.id: allocation_4
        }
        response = self._update(flight.id, put_data)
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        by_id = {
            campaign["id"]: campaign for campaign in response.data
        }
        self.assertEqual(by_id[1]["goal_allocation"], allocation_1)
        self.assertEqual(by_id[2]["goal_allocation"], allocation_2)
        self.assertEqual(by_id[3]["goal_allocation"], allocation_3)
        self.assertEqual(by_id[4]["goal_allocation"], allocation_4)

    def test_reject_invalid_allocation_values(self):
        self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        account = Account.objects.create(id=1)

        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement, account=account)
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement, account=account)

        allocation_1, allocation_2 = 30, "7!"
        put_data = {
            "flight_budget": 0,
            campaign_1.id: allocation_1,
            campaign_2.id: allocation_2
        }

        response = self._update(flight.id, put_data)
        campaign_1.refresh_from_db()
        campaign_2.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_allocation_within_margin(self):
        self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        account = Account.objects.create(id=1)

        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement, account=account, status="serving")
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement, account=account, status="serving")

        allocation_1, allocation_2 = 70, 31
        put_data = {
            "flight_budget": 100,
            campaign_1.id: allocation_1,
            campaign_2.id: allocation_2
        }
        response = self._update(flight.id, put_data)
        campaign_1.refresh_from_db()
        campaign_2.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        self.assertTrue(self.min_allocation <= sum([allocation_1, allocation_2]) <= self.max_allocation)

    def test_reject_allocation_outside_margin(self):
        self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        account = Account.objects.create(id=1)

        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement, account=account)
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement, account=account)
        campaign_3 = Campaign.objects.create(
            id=3, salesforce_placement=placement, account=account)
        campaign_4 = Campaign.objects.create(
            id=4, salesforce_placement=placement, account=account)

        allocation_1, allocation_2 = 70, 19
        allocation_3, allocation_4 = 90, 12
        put_data_1 = {
            "flight_budget": 0,
            campaign_1.id: allocation_1,
            campaign_2.id: allocation_2
        }
        put_data_2 = {
            "flight_budget": 0,
            campaign_1.id: allocation_3,
            campaign_2.id: allocation_4
        }
        response_1 = self._update(flight.id, put_data_1)
        response_2 = self._update(flight.id, put_data_2)
        campaign_1.refresh_from_db()
        campaign_2.refresh_from_db()
        campaign_3.refresh_from_db()
        campaign_4.refresh_from_db()

        self.assertEqual(response_1.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response_2.status_code, HTTP_400_BAD_REQUEST)
        self.assertTrue(sum([allocation_1, allocation_2]) <= self.min_allocation)
        self.assertTrue(sum([allocation_3, allocation_4]) >= self.max_allocation)

    def test_reject_allocation_min_budget(self):
        self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        account = Account.objects.create(id=1)

        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement, account=account)
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement, account=account)

        allocation_1, allocation_2 = 0.009, 100
        put_data = {
            "flight_budget": 100,
            campaign_1.id: allocation_1,
            campaign_2.id: allocation_2
        }
        response = self._update(flight.id, put_data)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_campaign_budget_history(self):
        user = self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        account = Account.objects.create(id=1)

        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement, account=account, status="serving")
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement, account=account, status="serving")
        campaign_3 = Campaign.objects.create(
            id=3, salesforce_placement=placement, account=account, status="serving")
        campaign_4 = Campaign.objects.create(
            id=4, salesforce_placement=placement, account=account, status="serving")

        allocation_1, allocation_2, allocation_3, allocation_4 = 70, 20, 5, 5
        put_data = {
            "flight_budget": 100,
            campaign_1.id: allocation_1,
            campaign_2.id: allocation_2,
            campaign_3.id: allocation_3,
            campaign_4.id: allocation_4
        }
        self.assertTrue(not CampaignHistory.objects.all().exists())
        response = self._update(flight.id, put_data)
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)

        history = [
            dict(
                campaign_id=h.campaign.id,
                budget=h.changes["budget"],
            ) for h in CampaignHistory.objects.filter(owner=user).order_by("id")
        ]
        self.assertEqual(history[0], dict(campaign_id=campaign_1.id, budget=allocation_1))
        self.assertEqual(history[1], dict(campaign_id=campaign_2.id, budget=allocation_2))
        self.assertEqual(history[2], dict(campaign_id=campaign_3.id, budget=allocation_3))
        self.assertEqual(history[3], dict(campaign_id=campaign_4.id, budget=allocation_4))
