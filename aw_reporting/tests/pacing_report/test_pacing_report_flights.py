from datetime import datetime
from datetime import time
from datetime import timedelta

import pytz
from django.conf import settings
from django.utils import timezone

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.reports.pacing_report import PacingReport
from utils.datetime import now_in_default_tz
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import patch_now


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
        tz_str = settings.DEFAULT_TIMEZONE
        tz = pytz.timezone(tz_str)
        now = datetime(2018, 1, 2, 3, 4, 5, tzinfo=tz)
        today = now.date()
        start = today - timedelta(days=2)
        end = today + timedelta(days=7)
        start_time = datetime.combine(start, time.min).replace(tzinfo=tz)
        end_time = datetime.combine(end + timedelta(days=1), time.min).replace(tzinfo=tz)
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
            start=start, end=end,
            cost=10
        )

        report = PacingReport()
        with patch_now(now):
            flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        first_data = flights[0]

        flight_total_minutes = (end_time - start_time).total_seconds() // 60
        flight_run_minutes = (now - start_time).total_seconds() // 60

        expected_cost = flight.cost * flight_run_minutes / flight_total_minutes
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
            flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        first_flight_data = flights[0]
        self.assertAlmostEqual(first_flight_data["pacing"], expected_pacing)

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
            items = report.get_flights(placement)
        self.assertEqual(len(items), 1)

        first_item_data = items[0]
        self.assertEqual(first_item_data["impressions"], delivered_units)
        self.assertEqual(first_item_data["cost"], cost)
        self.assertAlmostEqual(first_item_data["margin"], expected_margin)