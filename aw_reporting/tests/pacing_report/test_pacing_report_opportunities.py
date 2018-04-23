from datetime import timedelta, datetime

from django.utils import timezone

from aw_reporting.models import Opportunity, OpPlacement, SalesForceGoalType, \
    Flight, Campaign, CampaignStatistic
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import PacingReport
from utils.datetime import now_in_default_tz
from utils.utils_tests import ExtendedAPITestCase


class PacingReportOpportunitiesTestCase(ExtendedAPITestCase):

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
        today = now_in_default_tz().date()
        yesterday = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today, end=today, probability=100,
            budget=500001,
        )
        cpv_placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        campaign = Campaign.objects.create(id="1", name="",
                                           salesforce_placement=cpv_placement,
                                           video_views=1000, cost=5)
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign,
                                         video_views=1000, cost=5)
        Flight.objects.create(id="0", name="CPV Flight",
                              placement=cpv_placement,
                              start=yesterday, end=yesterday, cost=1)

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
            start=today - timedelta(days=2), end=today + timedelta(days=7),
            cost=10, total_cost=opportunity.budget / 3,
        )
        Flight.objects.create(
            id="3", name="", placement=placement,
            start=today + timedelta(days=12), end=today + timedelta(days=24),
            cost=100, total_cost=opportunity.budget / 3,
        )

        report = PacingReport()
        opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        first_op_data = opportunities[0]

        flight_total_days = (flight.end - flight.start).days + 1
        flight_run = (yesterday - flight.start).days + 1

        expected_cost = campaign.cost + prev_flight.cost + flight.cost * flight_run / flight_total_days
        self.assertEqual(first_op_data['cost'], expected_cost)

        expected_ordered_cost = cpv_placement.ordered_rate * campaign.video_views
        expected_margin = 1 - first_op_data['cost'] / expected_ordered_cost
        self.assertEqual(first_op_data['margin'], expected_margin)

    def test_pacing_opportunity_hard_cost_outgoing_fee(self):
        """
        Hard cost placements shouldn't affect Pacing
        :return:
        """
        today = now_in_default_tz().date()
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
            start=today - timedelta(days=2), end=today + timedelta(days=7),
            ordered_units=1000,
        )
        campaign = Campaign.objects.create(id="1", name="",
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

        report = PacingReport()
        opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        first_op_data = opportunities[0]
        self.assertEqual(first_op_data['video_views'], campaign.video_views)
        self.assertEqual(first_op_data['plan_video_views'],
                         flight.ordered_units * 1.02)
        self.assertEqual(first_op_data['pacing'], 1)

    def test_opportunity_chart_data(self):
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
            id="1", name="1", probability=100, budget=100, start=start, end=end,
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
        opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        opportunity_data = opportunities[0]["chart_data"]["cpv"]
        self.assertEqual(opportunity_data["today_goal"], 102)
        self.assertEqual(opportunity_data["today_budget"], 51)
        self.assertEqual(opportunity_data["yesterday_delivered"], 102)
        self.assertEqual(opportunity_data["yesterday_budget"], 51)

        charts = opportunity_data["charts"]
        self.assertEqual(len(charts), 2)

        pacing_chart = charts[0]
        self.assertEqual(pacing_chart["title"], "Ideal Pacing")
        self.assertEqual(len(pacing_chart["data"]),
                         (second_flight.end - first_flight.start).days + 1)
        self.assertEqual(pacing_chart["data"][3]["value"],
                         first_flight.ordered_units * 1.02)
        self.assertEqual(pacing_chart["data"][5]["value"],
                         first_flight.ordered_units * 1.02)
        self.assertEqual(pacing_chart["data"][-1]["value"],
                         first_flight.ordered_units * 1.02 + second_flight.ordered_units * 1.02)

        delivery_chart = charts[1]
        self.assertEqual(delivery_chart["title"], "Daily Deviation")
        self.assertEqual(len(delivery_chart["data"]), 3)
        self.assertEqual(delivery_chart["data"][-1]["value"], 306)

    def test_opportunity_cpv_and_cpm_chart_data(self):
        """
        We expect different chart blocks for every placement type (like CPV or CPM)
        :return:
        """
        start, end = datetime(2017, 1, 1).date(), datetime(2017, 1, 5).date()
        today = datetime(2017, 1, 4).date()
        opportunity = Opportunity.objects.create(
            id="1", name="1", probability=100, budget=100, start=start, end=end,
        )
        # CPV
        placement_cpv = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        flight_cpv = Flight.objects.create(
            id="1", name="", placement=placement_cpv, start=start, end=end,
            ordered_units=500,
        )
        campaign = Campaign.objects.create(id="1", name="",
                                           salesforce_placement=placement_cpv)
        for i in range(3):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start + timedelta(days=i),
                                             video_views=102, cost=51)
        # CPM
        placement_cpm = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM, start=start, end=end,
        )
        flight_cpm = Flight.objects.create(
            id="2", name="", placement=placement_cpm, start=start, end=end,
            ordered_units=5000,
        )
        campaign = Campaign.objects.create(id="2", name="",
                                           salesforce_placement=placement_cpm)
        for i in range(3):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start + timedelta(days=i),
                                             impressions=1020, cost=3)

        report = PacingReport(today=today)
        opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)

        opportunity_data = opportunities[0]

        # CPV tests
        cpv_chart_data = opportunity_data["chart_data"]["cpv"]
        self.assertEqual(cpv_chart_data["today_goal"], 102)
        self.assertEqual(cpv_chart_data["today_budget"], 51)
        self.assertEqual(cpv_chart_data["yesterday_delivered"], 102)
        self.assertEqual(cpv_chart_data["yesterday_budget"], 51)
        self.assertEqual(cpv_chart_data["targeting"]["video_views"], 306)

        charts = cpv_chart_data["charts"]
        self.assertEqual(len(charts), 2)

        pacing_chart = charts[0]
        self.assertEqual(pacing_chart["title"], "Ideal Pacing")
        self.assertEqual(len(pacing_chart["data"]), (end - start).days + 1)
        self.assertEqual(pacing_chart["data"][-1]["value"],
                         flight_cpv.ordered_units * 1.02)

        delivery_chart = charts[1]
        self.assertEqual(delivery_chart["title"], "Daily Deviation")
        self.assertEqual(len(delivery_chart["data"]), 3)
        self.assertEqual(delivery_chart["data"][-1]["value"], 306)

        # CPM tests
        cpm_chart_data = opportunity_data["chart_data"]["cpm"]
        self.assertEqual(cpm_chart_data["today_goal"], 1020)
        self.assertAlmostEqual(cpm_chart_data["today_budget"], 3, places=10)
        self.assertEqual(cpm_chart_data["yesterday_delivered"], 1020)
        self.assertEqual(cpm_chart_data["yesterday_budget"], 3)
        self.assertEqual(cpm_chart_data["targeting"]["impressions"], 3060)

        charts = cpm_chart_data["charts"]
        self.assertEqual(len(charts), 2)

        pacing_chart = charts[0]
        self.assertEqual(pacing_chart["title"], "Ideal Pacing")
        self.assertEqual(len(pacing_chart["data"]), (end - start).days + 1)
        self.assertEqual(pacing_chart["data"][-1]["value"],
                         flight_cpm.ordered_units * 1.02)

        delivery_chart = charts[1]
        self.assertEqual(delivery_chart["title"], "Daily Deviation")
        self.assertEqual(len(delivery_chart["data"]), 3)
        self.assertEqual(delivery_chart["data"][-1]["value"], 3060)

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
