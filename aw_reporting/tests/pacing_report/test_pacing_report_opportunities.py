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
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import PacingReport
from utils.datetime import now_in_default_tz
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import patch_now


class PacingReportOpportunitiesTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.user = self.create_test_user()

    def test_get_opportunities(self):
        today = timezone.now().date()
        yesterday = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=110,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1000,
            total_cost=10,
            start=yesterday, end=today,
        )
        campaign_1_delivery = dict(
            cost=1, video_views=100,  # cpv 0.01
            clicks=10, impressions=1000,
        )
        campaign = Campaign.objects.create(id="1", name="",
                                           salesforce_placement=placement,
                                           **campaign_1_delivery)
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign,
                                         **campaign_1_delivery)
        placement2 = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=.1,
        )
        flight2 = Flight.objects.create(
            id="2", name="", placement=placement2, ordered_units=1000,
            total_cost=100,
            start=yesterday, end=today,
        )
        campaign_2_delivery = dict(
            cost=1, video_views=10,  # cpv 0.1
            clicks=2, impressions=100,
        )
        campaign2 = Campaign.objects.create(id="2", name="",
                                            salesforce_placement=placement2,
                                            **campaign_2_delivery)
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign2,
                                         **campaign_2_delivery)

        report = PacingReport()
        opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        first_op_data = opportunities[0]

        self.assertEqual(first_op_data['id'], opportunity.id)

        self.assertEqual(first_op_data['cost'], campaign.cost + campaign2.cost)
        video_views = campaign.video_views + campaign2.video_views
        self.assertEqual(first_op_data['video_views'], video_views)
        clicks = campaign.clicks + campaign2.clicks
        self.assertEqual(first_op_data['ctr'], clicks / video_views)
        self.assertEqual(first_op_data['impressions'],
                         campaign.impressions + campaign2.impressions)

        self.assertEqual(first_op_data['plan_cost'], opportunity.budget)
        total_ordered_units = flight.ordered_units + flight2.ordered_units
        self.assertEqual(first_op_data['plan_cpv'], (
                flight.total_cost + flight2.total_cost) / total_ordered_units)
        self.assertEqual(first_op_data['plan_video_views'],
                         total_ordered_units * 1.02)
        self.assertEqual(first_op_data["margin"], 0,
                         "Ordered and delivered rates are same")

    def test_get_opportunities_over_50000(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=500001,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1000,
            total_cost=10, start=today, end=today,
        )

        report = PacingReport()
        opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        first_op_data = opportunities[0]
        self.assertEqual(first_op_data['plan_video_views'],
                         flight.ordered_units * 1.01)

    def test_margin_opportunity_hard_cost_outgoing_fee(self):
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
        account = Account.objects.create(timezone=tz_str, update_time=now)
        cpv_placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        campaign = Campaign.objects.create(id="1", name="",
                                           account=account,
                                           salesforce_placement=cpv_placement,
                                           video_views=1000, cost=5)
        CampaignStatistic.objects.create(date=start, campaign=campaign,
                                         video_views=1000, cost=5)
        Flight.objects.create(id="0", name="CPV Flight",
                              placement=cpv_placement,
                              start=start, end=end, cost=1)

        placement = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            placement_type=OpPlacement.OUTGOING_FEE_TYPE,
        )
        prev_flight = Flight.objects.create(
            id="1", name="", placement=placement,
            start=today - timedelta(days=5), end=today - timedelta(days=4),
            cost=1, total_cost=opportunity.budget / 3,
        )
        flight = Flight.objects.create(
            id="2", name="", placement=placement,
            start=start, end=end,
            cost=10, total_cost=opportunity.budget / 3,
        )
        Flight.objects.create(
            id="3", name="", placement=placement,
            start=today + timedelta(days=12), end=today + timedelta(days=24),
            cost=100, total_cost=opportunity.budget / 3,
        )

        report = PacingReport()
        with patch_now(now):
            opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        first_op_data = opportunities[0]

        flight_total_minutes = (end_time - start_time).total_seconds() // 60
        flight_run_minutes = (now - start_time).total_seconds() // 60

        expected_cost = campaign.cost + prev_flight.cost + flight.cost * flight_run_minutes / flight_total_minutes
        self.assertEqual(first_op_data['cost'], expected_cost)

        expected_ordered_cost = cpv_placement.ordered_rate * campaign.video_views
        expected_margin = 1 - first_op_data['cost'] / expected_ordered_cost
        self.assertEqual(first_op_data['margin'], expected_margin)

    def test_pacing_opportunity_hard_cost_outgoing_fee(self):
        """
        Hard cost placements shouldn't affect Pacing
        :return:
        """
        tz_str = settings.DEFAULT_TIMEZONE
        tz = pytz.timezone(tz_str)
        now = datetime(2018, 3, 4, 5, 6, 7, tzinfo=tz)
        today = now.date()
        start = today - timedelta(days=2)
        end = today + timedelta(days=7)
        update_time = datetime.combine(today, time.min).replace(tzinfo=tz)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=1000,
        )
        cpv_placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=cpv_placement,
            start=start, end=end,
            ordered_units=1000,
        )
        account = Account.objects.create(update_time=update_time)
        campaign = Campaign.objects.create(id="1", name="", account=account,
                                           salesforce_placement=cpv_placement,
                                           video_views=204)
        CampaignStatistic.objects.create(date=flight.start, campaign=campaign,
                                         video_views=204)

        placement = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            dynamic_placement=DynamicPlacementType.BUDGET,
            placement_type=OpPlacement.OUTGOING_FEE_TYPE,
        )
        Flight.objects.create(
            id="2", name="", placement=placement,
            start=today - timedelta(days=2), end=today + timedelta(days=7),
            ordered_units=10000,
        )

        with patch_now(now):
            report = PacingReport()
            opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        first_op_data = opportunities[0]
        self.assertEqual(first_op_data['video_views'], campaign.video_views)
        self.assertEqual(first_op_data['plan_video_views'],
                         flight.ordered_units * 1.02)
        self.assertEqual(first_op_data['pacing'], 1)

    def test_get_opportunities_has_dynamic_placements_type(self):
        today = now_in_default_tz().date()
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=today, end=today, probability=100,
            budget=110,
        )
        placement_1 = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
            dynamic_placement=DynamicPlacementType.BUDGET,
        )
        Flight.objects.create(
            id="1", name="", placement=placement_1, start=today, end=today,
        )

        placement_2 = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            dynamic_placement=DynamicPlacementType.SERVICE_FEE,
        )
        Flight.objects.create(
            id="2", name="", placement=placement_2, start=today, end=today,
        )

        placement_3 = OpPlacement.objects.create(
            id="3", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            dynamic_placement=DynamicPlacementType.BUDGET,
        )
        Flight.objects.create(
            id="3", name="", placement=placement_3, start=today, end=today,
        )

        report = PacingReport()
        opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        opportunity_data = opportunities[0]
        expected_types = {DynamicPlacementType.BUDGET,
                          DynamicPlacementType.SERVICE_FEE}
        actual_types = set(opportunity_data["dynamic_placements_types"])
        self.assertEqual(actual_types, expected_types)

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
            opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        first_op_data = opportunities[0]
        self.assertAlmostEqual(first_op_data["pacing"], expected_pacing)
