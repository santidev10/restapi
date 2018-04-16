from datetime import timedelta, date

from django.core.urlresolvers import reverse
from django.db.models import Sum
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED, \
    HTTP_404_NOT_FOUND

from aw_reporting.models import Opportunity, OpPlacement, Flight, \
    CampaignStatistic, Campaign, SalesForceGoalType, SalesForceGoalTypes, \
    DynamicPlacementType
from aw_reporting.reports.pacing_report import PacingReport
from utils.utils_tests import ExtendedAPITestCase as APITestCase, patch_now


class PacingReportOpportunitiesTestCase(APITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_forbidden_get(self):
        self.user.delete()
        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(1,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_not_found_get(self):
        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(1,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success_get(self):
        today = timezone.now()
        start = today - timedelta(days=3)
        end = today
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start,
            end=end,
        )
        placement = OpPlacement.objects.create(
            id="2", name="Where is my money", opportunity=opportunity,
            start=start, end=end,
        )
        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        item = data[0]
        self.assertEqual(
            set(item.keys()),
            {
                "id", "name", "start", "end", "goal_type_id", "is_upcoming",
                "is_completed", 'dynamic_placement',

                "pacing", "pacing_quality", "pacing_direction",
                "margin", "margin_quality", "margin_direction",
                "video_view_rate_quality", "ctr_quality",

                "plan_video_views", "plan_impressions",
                "plan_cpm", "plan_cpv", "goal_type", "plan_cost", "cost",

                "cpv", "cpm", "impressions", "video_views", "video_view_rate",
                "ctr",

                "targeting", "yesterday_budget", "today_goal", "today_budget",
                "yesterday_delivered", "charts",
                "today_goal_views", "yesterday_delivered_impressions",
                "today_goal_impressions", "yesterday_delivered_views"
            }
        )

        self.assertEqual(item["id"], placement.id)  # sorted chronologically

    def test_success_get_hard_cost_placement(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3))
        hard_cost_placement = OpPlacement.objects.create(
            id="2", name="Hard cost placement", opportunity=opportunity,
            start=today - timedelta(days=2), end=today + timedelta(days=2),
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(
            placement=hard_cost_placement, cost=30, total_cost=300)
        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        expected_null_fields = {
            "cpm", "cpv", "ctr", "ctr_quality", "impressions", "pacing",
            "pacing_direction", "pacing_quality", "plan_cmp", "plan_cpv",
            "plan_impressions", "plan_video_views", "video_view_rate",
            "video_view_rate_quality", "video_views"}
        opportunity_data = response.data[0]
        for key in expected_null_fields:
            self.assertIsNone(
                opportunity_data[key],
                "'{}' should be None for hard cost placement".format(key))
        self.assertEqual(opportunity_data["goal_type"],
                         SalesForceGoalTypes[hard_cost_placement.goal_type_id])
        self.assertEqual(opportunity_data["goal_type_id"],
                         hard_cost_placement.goal_type_id)

    def test_hard_cost_placement_margin_zero_total_cost(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3))
        hard_cost_placement = OpPlacement.objects.create(
            id="2", name="Hard cost placement", opportunity=opportunity,
            start=today - timedelta(days=2), end=today + timedelta(days=2),
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(
            start=today, end=today,
            placement=hard_cost_placement, total_cost=0, cost=1)
        Campaign.objects.create(
            salesforce_placement=hard_cost_placement)
        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data[0]["margin"], -100)

    def test_hard_cost_placement_margin_zero_cost(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3))
        hard_cost_placement = OpPlacement.objects.create(
            id="2", name="Hard cost placement", opportunity=opportunity,
            start=today - timedelta(days=2), end=today + timedelta(days=2),
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(
            placement=hard_cost_placement, cost=0, total_cost=10)
        Flight.objects.create(
            id="2", placement=hard_cost_placement, cost=0, total_cost=30)
        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data[0]["margin"], 100)

    def test_returns_alphabetical_ordered(self):
        today = timezone.now()
        start = today - timedelta(days=3)
        end = today + timedelta(days=3)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement_1 = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end
        )
        placement_2 = OpPlacement.objects.create(
            id="2", name="AAA", opportunity=opportunity, start=start, end=end
        )

        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data
        self.assertEqual(len(data), 2)
        response_ids = [op['id'] for op in data]
        self.assertEqual(response_ids, [placement_2.id, placement_1.id])

    def test_returns_dynamic_placement_type(self):
        today = timezone.now()
        start = today - timedelta(days=3)
        end = today + timedelta(days=3)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end,
            dynamic_placement=DynamicPlacementType.SERVICE_FEE
        )

        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        placement_data = response.data[0]
        self.assertEqual(placement_data["dynamic_placement"],
                         DynamicPlacementType.SERVICE_FEE)

    def test_dynamic_placement_bar_chart(self):
        now = timezone.now()
        today = now.date()
        yesterday = today - timedelta(days=1)
        start = today - timedelta(days=3)
        end = today + timedelta(days=3)
        total_cost = 8524
        days_left = (end - today).days + 1
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end,
            goal_type_id=SalesForceGoalType.CPM_AND_CPV,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement, start=start,
                              end=end,
                              total_cost=total_cost)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        yesterday_spent = 123
        CampaignStatistic.objects.create(campaign=campaign,
                                         cost=yesterday_spent, date=yesterday)
        CampaignStatistic.objects.create(campaign=campaign, cost=1023,
                                         date=yesterday - timedelta(days=1))
        total_spend = CampaignStatistic.objects \
            .filter(campaign__salesforce_placement=placement) \
            .aggregate(cost=Sum("cost"))["cost"]

        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        response = self.client.get(url)

        placement_data = response.data[0]
        expected_today_budget = (total_cost - total_spend) / days_left
        self.assertEqual(placement_data["yesterday_budget"], yesterday_spent)
        self.assertEqual(placement_data["today_budget"], expected_today_budget)

    def test_dynamic_placement_rate_tech_fee_chart_ideal_pacing(self):
        now = timezone.now()
        today = now.date()
        start = today
        end = today + timedelta(days=6)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start, end=end, total_cost=123,
            goal_type_id=SalesForceGoalType.CPM,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_type=OpPlacement.TECH_FEE_CPM_TYPE
        )
        total_cost = 1234
        Flight.objects.create(placement=placement, start=start, end=end,
                              total_cost=total_cost)

        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        ideal_pacing_chart = [c["data"] for c in response.data[0]["charts"]
                              if c["title"] == "Ideal Pacing"][0]

        actual_pacing = dict((str(i["label"]), i["value"])
                             for i in ideal_pacing_chart)
        total_days = (end - start).days + 1
        first_day_pacing = total_cost / total_days
        for i in range(total_days):
            day = start + timedelta(days=i)
            pacing = first_day_pacing * (i + 1)
            self.assertAlmostEqual(actual_pacing[str(day)], pacing,
                                   msg="wrong pacing for {} day".format(i + 1))

    def test_dynamic_placement_rate_tech_fee_plan_units(self):
        now = timezone.now()
        today = now.date()
        start = today - timedelta(days=3)
        end = today + timedelta(days=3)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start, end=end, total_cost=123,
            goal_type_id=SalesForceGoalType.CPM,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_type=OpPlacement.TECH_FEE_CPM_TYPE
        )
        total_cost = 1234
        ordered_units = 4321
        Flight.objects.create(placement=placement, start=start, end=end,
                              total_cost=total_cost,
                              ordered_units=ordered_units)

        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data[0]["plan_impressions"],
                         ordered_units * PacingReport.goal_factor)

    def test_dynamic_placement_budget(self):
        today = date(2017, 1, 1)
        start = today - timedelta(days=3)
        end = today + timedelta(days=3)
        total_days = (end - start).days + 1
        days_pass = (today - start).days
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        total_cost = 123
        aw_cost = 23
        views, impressions = 14, 164
        aw_cpv = aw_cost * 1. / views
        aw_cpm = aw_cost * 1000. / impressions
        expected_pacing = aw_cost / (total_cost / total_days * days_pass) * 100
        expected_marging = (total_cost - aw_cost) / total_cost * 100
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start, end=end, total_cost=total_cost,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.BUDGET,
        )
        Flight.objects.create(placement=placement, start=start, end=end,
                              total_cost=total_cost)
        campaign = Campaign.objects.create(salesforce_placement=placement,
                                           video_views=1)
        CampaignStatistic.objects.create(date=today, campaign=campaign,
                                         cost=aw_cost,
                                         video_views=views,
                                         impressions=impressions)
        url = reverse("aw_reporting_urls:pacing_report_placements",
                      args=(opportunity.id,))
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        pl = response.data[0]
        self.assertIsNone(pl["plan_video_views"])
        self.assertIsNone(pl["plan_impressions"])
        self.assertIsNone(pl["plan_cpv"])
        self.assertIsNone(pl["plan_cpm"])
        self.assertEqual(pl["plan_cost"], total_cost)
        self.assertEqual(pl["cost"], aw_cost)
        self.assertEqual(pl["cpv"], aw_cpv)
        self.assertEqual(pl["cpm"], aw_cpm)
        self.assertEqual(pl["impressions"], impressions)
        self.assertEqual(pl["video_views"], views)
        self.assertAlmostEqual(pl["pacing"], expected_pacing)
        self.assertAlmostEqual(pl["margin"], expected_marging)
