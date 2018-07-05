from datetime import timedelta, date
from itertools import product
from unittest import skipIf

from django.core.urlresolvers import reverse
from django.db.models import Sum
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED, \
    HTTP_404_NOT_FOUND

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Opportunity, OpPlacement, Flight, \
    CampaignStatistic, Campaign, SalesForceGoalType, SalesForceGoalTypes, \
    Account
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import PacingReportChartId, DefaultRate
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.utils_tests import ExtendedAPITestCase as APITestCase, patch_now, \
    get_current_release


class PacingReportPlacementsTestCase(APITestCase):
    @staticmethod
    def _get_url(*args):
        return reverse(
            Namespace.AW_REPORTING + ":" + Name.PacingReport.PLACEMENTS,
            args=args)

    def setUp(self):
        self.user = self.create_test_user()

    def test_forbidden_get(self):
        self.user.delete()
        url = self._get_url(1)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_not_found_get(self):
        url = self._get_url(1)
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
        url = self._get_url(opportunity.id)
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
                "ctr", "tech_fee",

                "targeting", "yesterday_budget", "today_goal", "today_budget",
                "yesterday_delivered", "charts",
                "today_goal_views", "yesterday_delivered_impressions",
                "today_goal_impressions", "yesterday_delivered_views",
                "current_cost_limit"
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
        url = self._get_url(opportunity.id)
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
        url = self._get_url(opportunity.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data[0]["margin"], -100)

    def test_hard_cost_placement_margin_zero_cost(self):
        now = now_in_default_tz()
        today = now.date()
        start, end = today - timedelta(days=2), today + timedelta(days=2)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3))
        hard_cost_placement = OpPlacement.objects.create(
            id="2", name="Hard cost placement", opportunity=opportunity,
            start=start, end=end,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(
            placement=hard_cost_placement, cost=0, total_cost=10,
            start=start, end=end)
        Flight.objects.create(
            id="2", placement=hard_cost_placement, cost=0, total_cost=30,
            start=start, end=end)
        url = self._get_url(opportunity.id)
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

        url = self._get_url(opportunity.id)
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

        url = self._get_url(opportunity.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        placement_data = response.data[0]
        self.assertEqual(placement_data["dynamic_placement"],
                         DynamicPlacementType.SERVICE_FEE)

    def test_dynamic_placement_bar_chart(self):
        now = now_in_default_tz()
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

        url = self._get_url(opportunity.id)
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

        url = self._get_url(opportunity.id)
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
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start, end=end, total_cost=total_cost,
            ordered_rate=123,
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
        url = self._get_url(opportunity.id)
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
        self.assertAlmostEqual(pl["margin"], 0)

    def test_dynamic_placement_rate_and_tech_fee(self):
        today = date(2017, 1, 1)
        yesterday = today - timedelta(days=1)
        start = today - timedelta(days=3)
        end = today + timedelta(days=5)
        total_duration = (end - start).days + 1
        days_passed = (yesterday - start).days + 1
        tech_fee = 0.12
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        total_cost = 123
        aw_cost = 23
        clicks, views, impressions = 3, 14, 164
        aw_cpv = aw_cost * 1. / views
        aw_cpm = aw_cost * 1000. / impressions
        rate = 2.3
        expected_margin = tech_fee * 100. / (aw_cpv + tech_fee)
        video_view_rate = views * 100. / impressions
        ctr = clicks * 100. / views
        planned_cost = total_cost / total_duration * days_passed
        expected_pacing = aw_cost * 100. / planned_cost

        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start, end=end, total_cost=total_cost,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=rate,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=tech_fee
        )
        Flight.objects.create(placement=placement, start=start, end=end,
                              total_cost=total_cost)
        campaign = Campaign.objects.create(salesforce_placement=placement,
                                           video_views=1)
        CampaignStatistic.objects.create(date=today - timedelta(days=1),
                                         campaign=campaign,
                                         cost=aw_cost,
                                         clicks=clicks,
                                         video_views=views,
                                         impressions=impressions)
        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        pl = response.data[0]
        self.assertEqual(pl["dynamic_placement"],
                         DynamicPlacementType.RATE_AND_TECH_FEE)
        self.assertAlmostEqual(pl["tech_fee"], tech_fee)
        # contracted rate
        self.assertIsNone(pl["plan_cpv"])
        self.assertIsNone(pl["plan_cpm"])
        # client budget
        self.assertEqual(pl["plan_cost"], total_cost)
        # amount spent
        self.assertEqual(pl["cost"], aw_cost)
        # view rate
        self.assertEqual(pl["video_view_rate"], video_view_rate)

        self.assertEqual(pl["ctr"], ctr)
        self.assertEqual(pl["cpv"], aw_cpv)
        self.assertEqual(pl["cpm"], aw_cpm)
        self.assertEqual(pl["impressions"], impressions)
        self.assertEqual(pl["video_views"], views)
        self.assertAlmostEqual(pl["margin"], expected_margin)
        self.assertAlmostEqual(pl["pacing"], expected_pacing)

    def test_dynamic_placement_budget_charts_ideal_pacing(self):
        today = date(2017, 1, 15)
        start = today - timedelta(days=1)
        end = today + timedelta(days=1)
        total_cost = 12

        expected_ideal_pacing = [
            dict(value=4, label=start),  # yesterday
            dict(value=6, label=today),  # today
            dict(value=12, label=end),  # tomorrow
        ]
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement, start=start,
                              end=end,
                              total_cost=total_cost)
        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        flight_data = response.data[0]
        self.assertEqual(flight_data["plan_cost"], total_cost)
        self.assertIsNotNone(flight_data["charts"])
        charts = dict((c["id"], c["data"]) for c in flight_data["charts"])
        ideal_pacing = charts.get(PacingReportChartId.IDEAL_PACING, [])
        charts_zipped = zip(ideal_pacing, expected_ideal_pacing)
        for actual, expected in charts_zipped:
            label = expected["label"]
            self.assertEqual(actual["label"], label)
            self.assertAlmostEqual(actual["value"], expected["value"],
                                   msg=label)

    def test_dynamic_placement_service_fee_charts_ideal_pacing(self):
        today = date(2017, 1, 15)
        start = today - timedelta(days=1)
        end = today + timedelta(days=1)
        total_cost = 12

        expected_ideal_pacing = [
            dict(value=4, label=start),  # yesterday
            dict(value=6, label=today),  # today
            dict(value=12, label=end),  # tomorrow
        ]
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end,
            dynamic_placement=DynamicPlacementType.SERVICE_FEE,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement, start=start,
                              end=end,
                              total_cost=total_cost)
        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        flight_data = response.data[0]
        self.assertEqual(flight_data["plan_cost"], total_cost)
        self.assertIsNotNone(flight_data["charts"])
        charts = dict((c["id"], c["data"]) for c in flight_data["charts"])
        ideal_pacing = charts.get(PacingReportChartId.IDEAL_PACING, [])
        charts_zipped = zip(ideal_pacing, expected_ideal_pacing)
        for actual, expected in charts_zipped:
            label = expected["label"]
            self.assertEqual(actual["label"], label)
            self.assertAlmostEqual(actual["value"], expected["value"],
                                   msg=label)

    def test_dynamic_placement_service_fee_daily_data_budget(self):
        today = date(2017, 1, 15)
        yesterday = today - timedelta(days=1)
        start = today - timedelta(days=1)
        end = today + timedelta(days=10)
        days_left = (end - today).days + 1
        total_cost = 1234
        total_spend = yesterday_spend = 32
        today_goal = (total_cost - total_spend) / days_left

        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end,
            dynamic_placement=DynamicPlacementType.SERVICE_FEE,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement, start=start,
                              end=end,
                              total_cost=total_cost)
        campaign = Campaign.objects.create(id=1, salesforce_placement=placement)
        CampaignStatistic.objects.create(date=yesterday,
                                         campaign=campaign,
                                         cost=yesterday_spend)
        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        flight_data = response.data[0]
        self.assertEqual(flight_data["yesterday_budget"], yesterday_spend)
        self.assertEqual(flight_data["today_budget"], today_goal)

    def test_dynamic_placement_rate_and_tech_fee_no_statistic(self):
        today = date(2017, 1, 1)
        start = today
        end = today + timedelta(days=8)
        duration = (end - start).days + 1
        tech_fee = 0.12
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        total_cost = 123
        rate = 2.3
        goal = DefaultRate.CPV / (DefaultRate.CPV + tech_fee) * total_cost
        today_goal = goal / duration
        daily_goal = total_cost / duration
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start, end=end, total_cost=total_cost,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=rate,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=tech_fee
        )
        Flight.objects.create(placement=placement, start=start, end=end,
                              total_cost=total_cost)
        Campaign.objects.create(salesforce_placement=placement,
                                video_views=1)
        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        pl = response.data[0]
        ideal_pacing_chart = dict((c["id"], c["data"])
                                  for c in pl["charts"]) \
            .get(PacingReportChartId.IDEAL_PACING, [])
        self.assertEqual(len(ideal_pacing_chart), duration)

        self.assertEqual(pl["dynamic_placement"],
                         DynamicPlacementType.RATE_AND_TECH_FEE)
        self.assertIsNone(pl["plan_video_views"])
        self.assertIsNone(pl["plan_impressions"])
        self.assertEqual(pl["today_budget"], today_goal)

        self.assertIsNotNone(pl["charts"])
        charts = dict((c["id"], c["data"]) for c in pl["charts"])
        ideal_pacing = charts.get(PacingReportChartId.IDEAL_PACING, [])
        pacing_values = [c["value"] for c in ideal_pacing]
        expected_chart = [(i + 1) * daily_goal for i in range(duration)]
        for actual, expected in zip(pacing_values, expected_chart):
            self.assertAlmostEqual(actual, expected)

    def test_dynamic_placement_service_fee(self):
        today = date(2017, 1, 1)
        start = today - timedelta(days=3)
        end = today + timedelta(days=3)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        total_cost = 123
        aw_cost = 23
        views, impressions = 14, 164
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start, end=end, total_cost=total_cost,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.SERVICE_FEE,
        )
        Flight.objects.create(placement=placement, start=start, end=end,
                              total_cost=total_cost)
        campaign = Campaign.objects.create(salesforce_placement=placement,
                                           video_views=1)
        CampaignStatistic.objects.create(date=today, campaign=campaign,
                                         cost=aw_cost,
                                         video_views=views,
                                         impressions=impressions)
        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        pl = response.data[0]
        self.assertEqual(pl["plan_cost"], placement.total_cost)

    def test_dynamic_placement_budget_over_delivery_margin(self):
        today = date(2017, 1, 1)
        start = today - timedelta(days=3)
        end = today + timedelta(days=3)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        total_cost = 123
        aw_cost = 234
        assert aw_cost > total_cost
        expected_margin = (total_cost - aw_cost) / total_cost * 100
        views, impressions = 14, 164
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
        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        pl = response.data[0]
        self.assertAlmostEqual(pl["margin"], expected_margin)

    def test_dynamic_placement_ordered_rate(self):
        today = date(2017, 1, 1)
        start = today
        end = today + timedelta(days=8)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )

        def create_placement_and_flight(uid, dynamic_placement, goal_type_id):
            placement = OpPlacement.objects.create(
                id=uid, opportunity=opportunity,
                start=start, end=end, total_cost=12,
                goal_type_id=goal_type_id,
                ordered_rate=2.1,
                dynamic_placement=dynamic_placement,
                tech_fee=0.02
            )
            Flight.objects.create(id=uid, placement=placement, start=start,
                                  end=end,
                                  total_cost=12)
            return placement

        test_data = list(product(
            (
                DynamicPlacementType.BUDGET,
                DynamicPlacementType.SERVICE_FEE,
                DynamicPlacementType.RATE_AND_TECH_FEE
            ),
            (
                SalesForceGoalType.CPV,
                SalesForceGoalType.CPM
            )
        ))

        for i, data in enumerate(test_data):
            dynamic_type, goal_type_id = data
            create_placement_and_flight(i, dynamic_type,
                                        goal_type_id)

        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), len(list(test_data)))
        for pl in response.data:
            self.assertIsNone(pl["plan_cpm"],
                              "plan_cpm, {}, {}".format(pl["goal_type"], pl[
                                  "dynamic_placement"]))
            self.assertIsNone(pl["plan_cpv"],
                              "plan_cpv, {}, {}".format(pl["goal_type"], pl[
                                  "dynamic_placement"]))

    def test_dynamic_placement_budget_margin(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2435
        Summary:
        Pacing report > Margin should be 0% for Dynamic placement: Budget
        if there was no over delivery
        :return:
        """
        today = date(2018, 5, 14)
        start, end = date(2018, 5, 10), date(2018, 6, 10)
        total_cost = 6042.9
        opportunity = Opportunity.objects.create(id=1, probability=100)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=total_cost,
            start=start, end=end)
        Flight.objects.create(id=1,
                              placement=placement,
                              start=start, end=end,
                              total_cost=total_cost)
        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        pl_data = response.data[0]
        self.assertEqual(pl_data["margin"], 0)

    def test_fix_distinct(self):
        opportunity = Opportunity.objects.create(id=1)
        placement_1 = OpPlacement.objects.create(id=1, opportunity=opportunity)
        placement_2 = OpPlacement.objects.create(id=2, opportunity=opportunity)
        account_1 = Account.objects.create(id=1)
        account_2 = Account.objects.create(id=2)
        Campaign.objects.create(id=1, salesforce_placement=placement_1,
                                account=account_1)
        Campaign.objects.create(id=2, salesforce_placement=placement_2,
                                account=account_2)

        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account_1.id, account_2.id]
        }
        url = self._get_url(opportunity.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

    def test_hard_cost_margin_start(self):
        today = date(2018, 1, 1)
        total_cost = 6543
        our_cost = 1234
        start = today - timedelta(days=1)
        end = today + timedelta(days=1)
        self.assertGreater(today, start)
        opportunity = Opportunity.objects.create(
            id="1", name="1")
        hard_cost_placement = OpPlacement.objects.create(
            id="2", name="Hard cost placement", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(
            start=start, end=end, total_cost=total_cost,
            placement=hard_cost_placement, cost=our_cost)
        Flight.objects.create(id=2,
                              start=today + timedelta(days=1),
                              end=today + timedelta(days=1),
                              total_cost=999999,
                              placement=hard_cost_placement, cost=0)
        client_cost = total_cost
        expected_margin = (1 - our_cost / client_cost) * 100
        url = self._get_url(opportunity.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data[0]["margin"], expected_margin)
