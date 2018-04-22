from datetime import timedelta, datetime
from unittest import skip

from django.utils import timezone

from aw_reporting.models import Opportunity, OpPlacement, SalesForceGoalType, \
    Flight, Campaign, CampaignStatistic
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import PacingReport
from utils.datetime import now_in_default_tz
from utils.utils_tests import ExtendedAPITestCase, patch_now


class PacingReportTestCase(ExtendedAPITestCase):

    def test_get_flights_budget_under_50000(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=50000,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        flight = Flight.objects.create(
            id="1", name="", start=today, end=today, placement=placement, ordered_units=1000, total_cost=10,
        )

        report = PacingReport()
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        data = flights[0]
        self.assertEqual(data['plan_video_views'], flight.ordered_units * 1.02)

    def test_get_flights_budget_over_50000(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=500001,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        flight = Flight.objects.create(
            id="1", name="", start=today, end=today, placement=placement, ordered_units=1000, total_cost=10,
        )

        report = PacingReport()
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        data = flights[0]
        self.assertEqual(data['plan_video_views'], flight.ordered_units * 1.01)

    def test_cost_hard_cost_outgoing_fee_completed(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=500001,
        )
        placement = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST, placement_type=OpPlacement.OUTGOING_FEE_TYPE,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1,
            start=today - timedelta(days=5), end=today - timedelta(days=4), cost=1,
        )

        report = PacingReport()
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        first_data = flights[0]
        self.assertEqual(first_data['cost'], flight.cost)

    def test_cost_hard_cost_outgoing_fee_running(self):
        today = now_in_default_tz().date()
        yesterday = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=500001,
        )
        placement = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST, placement_type=OpPlacement.OUTGOING_FEE_TYPE,
        )
        flight = Flight.objects.create(
            id="2", name="", placement=placement, ordered_units=1,
            start=today - timedelta(days=2), end=today + timedelta(days=7),
            cost=10
        )

        report = PacingReport()
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        first_data = flights[0]

        flight_total_days = (flight.end - flight.start).days + 1
        flight_run = (yesterday - flight.start).days + 1

        expected_cost = flight.cost * flight_run / flight_total_days
        self.assertEqual(first_data['cost'], expected_cost)

    def test_cost_hard_cost_outgoing_fee_upcoming(self):
        today = now_in_default_tz().date()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=500001,
        )
        placement = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST, placement_type=OpPlacement.OUTGOING_FEE_TYPE,
        )
        Flight.objects.create(
            id="3", name="", placement=placement, ordered_units=1,
            start=today + timedelta(days=12), end=today + timedelta(days=24),
            cost=100
        )

        report = PacingReport()
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        first_data = flights[0]

        self.assertEqual(first_data['cost'], 0)

    @skip("Enable in scope of ticket SAAS-2305")
    def test_dynamic_placement_added_fee(self):
        """
        Opportunity
            Placement Budget: total_cost=80000, dynamic_placement=Rate + Tech Fee
                Flight: total_cost=80000
        :return:
        """
        start, end = datetime(2017, 1, 1).date(), datetime(2017, 1, 8).date()
        today = datetime(2017, 1, 4).date()
        opportunity = Opportunity.objects.create(id="1", name="", probability=100)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.HARD_COST, start=start, end=end,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE, total_cost=80000,
            tech_fee=0.01

        )
        flight = Flight.objects.create(id="2", name="", placement=placement, start=start,
                                       end=end, total_cost=placement.total_cost)

        campaign = Campaign.objects.create(id="1", name="", video_views=1, salesforce_placement=placement)
        delivery_stats = dict(video_views=100000, cost=5000)
        delivery_days = 3
        for i in range(delivery_days):
            CampaignStatistic.objects.create(campaign=campaign, date=start + timedelta(days=i), **delivery_stats)

        report = PacingReport(today=today)
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        flight_data = flights[0]
        self.assertEqual(flight_data["id"], flight.id)
        delivered_cost = delivery_stats["cost"] * delivery_days
        self.assertEqual(flight_data["cost"], delivered_cost)
        cpv = delivery_stats["cost"] / delivery_stats["video_views"]
        self.assertEqual(flight_data["cpv"], cpv)
        margin = placement.tech_fee / (cpv + placement.tech_fee)
        self.assertAlmostEqual(flight_data["margin"], margin, places=10)

        # pacing
        delivery_co = cpv / (placement.tech_fee + cpv)  # if margin 0.16 then it's 0.84
        budget_to_spend = delivery_co * flight.total_cost
        total_days = (end - start).days + 1
        budget_by_yesterday = budget_to_spend * delivery_days / total_days
        self.assertEqual(flight_data["pacing"], delivered_cost / budget_by_yesterday)

        self.assertEqual(flight_data["today_goal"], None)
        self.assertEqual(flight_data["today_budget"], None)
        self.assertEqual(flight_data["yesterday_delivered"], None)
        self.assertEqual(flight_data["yesterday_budget"], delivery_stats["cost"])

        self.assertEqual(flight_data["charts"], None)

    def test_cpv_flight_ended_pacing_chart(self):
        start, end = datetime(2017, 1, 1).date(), datetime(2017, 1, 8).date()
        today = datetime(2017, 1, 8).date()
        opportunity = Opportunity.objects.create(id="1", name="",
                                                 probability=100)
        placement = OpPlacement.objects.create(
            id="1", name="Second", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
            ordered_units=8000, total_cost=80000,
        )
        Flight.objects.create(id="1", name="", placement=placement, start=start,
                              end=end, total_cost=placement.total_cost,
                              ordered_units=placement.ordered_units)

        campaign = Campaign.objects.create(id="1", name="", video_views=1,
                                           salesforce_placement=placement)
        for i in range(8):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start + timedelta(days=i),
                                             video_views=2040)

        report = PacingReport(today=today)
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        flight_data = flights[0]
        charts = flight_data["charts"]
        self.assertEqual(len(charts), 2)
        pacing_chart = charts[0]
        self.assertEqual(pacing_chart["title"], "Ideal Pacing")

        self.assertEqual(len(pacing_chart["data"]), (end - start).days + 1)
        self.assertEqual(pacing_chart["data"][0]["value"], 1020)
        self.assertEqual(pacing_chart["data"][2]["value"], 4760)
        self.assertEqual(pacing_chart["data"][3]["value"], 6528)
        self.assertEqual(pacing_chart["data"][4]["value"], 8160)
        self.assertEqual(pacing_chart["data"][-1]["value"], placement.ordered_units * 1.02)
