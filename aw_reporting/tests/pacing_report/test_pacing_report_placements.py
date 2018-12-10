from datetime import datetime
from datetime import time
from datetime import timedelta

import pytz
from django.utils import timezone

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import settings
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import PacingReport
from utils.datetime import now_in_default_tz
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.patch_now import patch_now


class PacingReportPlacementsTestCase(ExtendedAPITestCase):

    def test_get_placements_budget_under_50000(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=50000,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1000, total_cost=10, start=today, end=today,
        )

        report = PacingReport()
        placements = report.get_placements(opportunity)
        self.assertEqual(len(placements), 1)

        data = placements[0]
        self.assertEqual(data['plan_video_views'], flight.ordered_units * 1.02)

    def test_get_placements_budget_over_50000(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=500001,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1000, total_cost=10, start=today, end=today,
        )

        report = PacingReport()
        placements = report.get_placements(opportunity)
        self.assertEqual(len(placements), 1)

        data = placements[0]
        self.assertEqual(data['plan_video_views'], flight.ordered_units * 1.01)

    def test_cost_hard_cost_outgoing_fee(self):
        today = now_in_default_tz().date()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=500001)
        placement = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            placement_type=OpPlacement.OUTGOING_FEE_TYPE
        )
        prev_flight = Flight.objects.create(
            id="1", name="", placement=placement,
            start=today - timedelta(days=5), end=today - timedelta(days=4),
            cost=1)
        flight_1 = Flight.objects.create(
            id="2", name="", placement=placement,
            start=today - timedelta(days=2), end=today + timedelta(days=7),
            cost=10)
        flight_2 = Flight.objects.create(
            id="3", name="", placement=placement,
            start=today + timedelta(days=12), end=today + timedelta(days=24),
            cost=100)
        report = PacingReport()
        placements = report.get_placements(opportunity)
        self.assertEqual(len(placements), 1)
        expected_cost = sum(
            (obj.cost for obj in [prev_flight, flight_1, flight_2]))
        first_data = placements[0]
        self.assertEqual(first_data["cost"], expected_cost)

    def test_placement_chart_data(self):
        """
        Daily Pacing
        {'label': datetime.date(2017, 1, 1), 'value': 102.0}
        {'label': datetime.date(2017, 1, 2), 'value': 204.0}
        {'label': datetime.date(2017, 1, 3), 'value': 306.0}
        {'label': datetime.date(2017, 1, 4), 'value': 408.0}
        {'label': datetime.date(2017, 1, 5), 'value': 408.0}
        {'label': datetime.date(2017, 1, 6), 'value': 408.0}
        {'label': datetime.date(2017, 1, 7), 'value': 408.0+51.0=459.0}
        {'label': datetime.date(2017, 1, 8), 'value': 408.0+102.0=510.0}

        Delivery Chart
        {'label': datetime.date(2017, 1, 1), 'value': 102}
        {'label': datetime.date(2017, 1, 2), 'value': 204}
        {'label': datetime.date(2017, 1, 3), 'value': 306}

        2017-01-04
        Today goal: 102
        Today budget: 102 * 0.5
        :return:
        """
        start, end = datetime(2017, 1, 1).date(), datetime(2017, 1, 8).date()
        today = datetime(2017, 1, 4).date()
        opportunity = Opportunity.objects.create(
            id="1", name="1", probability=100, budget=100,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        first_end = start + timedelta(days=3)
        first_flight = Flight.objects.create(
            id="1", name="", placement=placement,
            start=start, end=first_end, ordered_units=400,
        )

        second_start = first_end + timedelta(days=3)
        second_flight = Flight.objects.create(
            id="2", name="", placement=placement,
            start=second_start, end=end, ordered_units=100,
        )

        campaign = Campaign.objects.create(id="1", name="",
                                           salesforce_placement=placement)
        for i in range(3):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start + timedelta(days=i),
                                             video_views=102, cost=51)

        report = PacingReport(today=today)
        placements = report.get_placements(opportunity)
        self.assertEqual(len(placements), 1)

        placement_data = placements[0]
        self.assertEqual(placement_data["today_goal"], 102)
        self.assertEqual(placement_data["today_budget"], 51)
        self.assertEqual(placement_data["yesterday_delivered"], 102)
        self.assertEqual(placement_data["yesterday_budget"], 51)

        charts = placement_data["charts"]
        self.assertEqual(len(charts), 4)

        pacing_chart = charts[0]
        self.assertEqual(pacing_chart["title"], "Ideal Pacing")
        self.assertEqual(len(pacing_chart["data"]), (second_flight.end - first_flight.start).days + 1)
        self.assertEqual(pacing_chart["data"][3]["value"], first_flight.ordered_units * 1.02)
        self.assertEqual(pacing_chart["data"][4]["value"], first_flight.ordered_units * 1.02)
        self.assertEqual(pacing_chart["data"][5]["value"], first_flight.ordered_units * 1.02)
        self.assertEqual(pacing_chart["data"][-1]["value"],
                         first_flight.ordered_units * 1.02 + second_flight.ordered_units * 1.02)

        delivery_chart = charts[1]
        self.assertEqual(delivery_chart["title"], "Daily Deviation")
        self.assertEqual(len(delivery_chart["data"]), 3)
        self.assertEqual(delivery_chart["data"][-1]["value"], 306)

    def test_placement_dynamic_placement(self):
        today = timezone.now().date()
        yesterday = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=today, end=today, probability=100, budget=110,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        Flight.objects.create(
            id="1", name="", placement=placement, start=yesterday, end=yesterday,
        )
        placement2 = OpPlacement.objects.create(
            id="2", name="B", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            dynamic_placement=DynamicPlacementType.BUDGET,
        )
        Flight.objects.create(
            id="2", name="", placement=placement2, start=yesterday, end=yesterday,
        )

        report = PacingReport()
        placements = report.get_placements(opportunity)
        self.assertEqual(len(placements), 2)

        first = placements[0]
        self.assertEqual(first["id"], placement.id)
        self.assertIs(first["dynamic_placement"], None)

        second = placements[1]
        self.assertEqual(second["id"], placement2.id)
        self.assertEqual(second["dynamic_placement"], DynamicPlacementType.BUDGET)

    def test_pacing_calculated_by_minutes(self):
        test_timezone_str = settings.DEFAULT_TIMEZONE
        test_timezone = pytz.timezone(test_timezone_str)
        test_now = datetime(2018, 1, 1, 14, 45, tzinfo=pytz.utc)
        test_last_update = test_now - timedelta(hours=3)
        self.assertEqual(test_now.date(), test_last_update.date())
        start = (test_now - timedelta(days=5)).date()
        end = (test_now + timedelta(days=2)).date()
        ordered_units = 1234
        delivered_units = 345

        opportunity = Opportunity.objects.create(probability=100)
        placement = OpPlacement.objects.create(opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM)
        Flight.objects.create(placement=placement, start=start, end=end, ordered_units=ordered_units)

        account = Account.objects.create(update_time=test_last_update, timezone=test_timezone_str)
        campaign = Campaign.objects.create(account=account, salesforce_placement=placement)
        CampaignStatistic.objects.create(date=test_now, campaign=campaign, impressions=delivered_units)

        start_time = datetime.combine(start, time.min).replace(tzinfo=test_timezone)
        end_time = datetime.combine(end + timedelta(days=1), time.min).replace(tzinfo=test_timezone)
        total_minutes = (end_time - start_time).total_seconds() // 60
        minutes_passed = (test_last_update - start_time).total_seconds() // 60
        total_plan_units = ordered_units * PacingReport.goal_factor
        planned_units = total_plan_units / total_minutes * minutes_passed
        expected_pacing = delivered_units / planned_units

        with patch_now(test_now):
            report = PacingReport()
            placements = report.get_placements(opportunity)
        self.assertEqual(len(placements), 1)

        first_placement_data = placements[0]
        self.assertAlmostEqual(first_placement_data["pacing"], expected_pacing)

    def test_aggregates_latest_data(self):
        test_now = datetime(2018, 1, 1, 14, 45, tzinfo=pytz.utc)
        start = (test_now - timedelta(days=5)).date()
        end = (test_now + timedelta(days=2)).date()
        ordered_units = 1234
        delivered_units = 345
        cost = 123

        opportunity = Opportunity.objects.create(probability=100)
        placement = OpPlacement.objects.create(opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
                                               ordered_rate=12.2)
        Flight.objects.create(placement=placement, start=start, end=end,
                              ordered_units=ordered_units, total_cost=9999)

        account = Account.objects.create()
        campaign = Campaign.objects.create(account=account, salesforce_placement=placement)
        CampaignStatistic.objects.create(date=test_now, campaign=campaign, impressions=delivered_units, cost=cost)

        client_cost = delivered_units * placement.ordered_rate / 1000
        expected_margin = 1 - cost / client_cost

        with patch_now(test_now):
            report = PacingReport()
            items = report.get_placements(opportunity)
        self.assertEqual(len(items), 1)

        first_item_data = items[0]
        self.assertEqual(first_item_data["impressions"], delivered_units)
        self.assertEqual(first_item_data["cost"], cost)
        self.assertAlmostEqual(first_item_data["margin"], expected_margin)
