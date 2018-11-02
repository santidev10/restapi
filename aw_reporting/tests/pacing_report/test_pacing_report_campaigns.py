from datetime import timedelta, datetime

from django.utils import timezone

from aw_reporting.models import Opportunity, OpPlacement, SalesForceGoalType, \
    Flight, Account, Campaign, CampaignStatistic
from aw_reporting.reports.pacing_report import PacingReport
from utils.utils_tests import ExtendedAPITestCase


class PacingReportTestCase(ExtendedAPITestCase):

    def test_get_success(self):
        today = timezone.now()
        start = today - timedelta(days=4)
        end = today - timedelta(days=2)
        opportunity = Opportunity.objects.create(id="1", name="1", start=start, end=end, probability=100, budget=5000)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV, ordered_rate=.01,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1000, total_cost=10, start=start, end=end,
        )
        account = Account.objects.create(id="1", name="")
        stats_1 = dict(impressions=1000, video_views=10, cost=1, clicks=1)
        campaign_1 = Campaign.objects.create(id="1", name="A", account=account,
                                             salesforce_placement=placement, **stats_1)
        CampaignStatistic.objects.create(campaign=campaign_1, date=start, **stats_1)

        stats_2 = dict(impressions=500, video_views=2, cost=1, clicks=0)
        campaign_2 = Campaign.objects.create(id="2", name="B", account=account,
                                             salesforce_placement=placement, **stats_2)
        CampaignStatistic.objects.create(campaign=campaign_2, date=start, **stats_2)

        report = PacingReport()
        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 2)

        first = campaigns[0]
        self.assertEqual(first['impressions'], stats_1["impressions"])
        self.assertEqual(first['video_views'], stats_1["video_views"])
        self.assertEqual(first['cost'], stats_1["cost"])
        self.assertEqual(first['cpv'], stats_1["cost"] / stats_1["video_views"])
        self.assertEqual(first['cpm'], stats_1["cost"] / stats_1["impressions"] * 1000)
        self.assertEqual(first['ctr'], stats_1["clicks"] / stats_1["video_views"])
        self.assertEqual(first['video_view_rate'], stats_1["video_views"] / stats_1["impressions"])

    def test_get_budget_under_50000(self):
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
        Campaign.objects.create(id="1", name="", account=Account.objects.create(id="1", name=""),
                                salesforce_placement=placement)

        report = PacingReport()
        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 1)

        data = campaigns[0]
        self.assertEqual(data['plan_video_views'], flight.ordered_units * 1.02)

    def test_get_budget_over_50000(self):
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
        Campaign.objects.create(id="1", name="", account=Account.objects.create(id="1", name=""),
                                salesforce_placement=placement)

        report = PacingReport()
        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 1)

        data = campaigns[0]
        self.assertEqual(data['plan_video_views'], flight.ordered_units * 1.01)

    def test_campaigns_goal_allocation(self):
        """
        CASE:
        today = March 1st
        :return:
        """
        today = datetime(2017, 3, 1).date()
        opportunity = Opportunity.objects.create(id='1', name="", start=today, end=today)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
        )
        campaign_1 = Campaign.objects.create(id="1", name="Alpha", salesforce_placement=placement)
        campaign_2 = Campaign.objects.create(id="2", name="Beta", salesforce_placement=placement)

        end = today.replace(day=30)
        flight = Flight.objects.create(id="1", name="", placement=placement, ordered_units=1000, start=today, end=end)

        # check
        report = PacingReport(today=today)
        campaigns = report.get_campaigns(flight)

        campaign_data = campaigns[0]
        self.assertEqual(campaign_data["id"], campaign_1.id)
        self.assertEqual(campaign_data['plan_video_views'], 510)
        self.assertEqual(campaign_data['today_goal'], 510 / 30)
        first_chart = campaign_data['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], 510)

        campaign_data = campaigns[1]
        self.assertEqual(campaign_data["id"], campaign_2.id)
        self.assertEqual(campaign_data['plan_video_views'], 510)
        self.assertEqual(campaign_data['today_goal'], 510 / 30)
        first_chart = campaign_data['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], 510)
