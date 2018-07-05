from datetime import timedelta, date, datetime
from itertools import product
from unittest import skipIf

from django.core.urlresolvers import reverse
from django.db.models import Sum
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED, \
    HTTP_404_NOT_FOUND

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Opportunity, OpPlacement, Flight, \
    SalesForceGoalType, Campaign, CampaignStatistic, goal_type_str
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import PacingReportChartId, DefaultRate
from saas.urls.namespaces import Namespace
from utils.datetime import now_in_default_tz
from utils.utils_tests import ExtendedAPITestCase as APITestCase, patch_now, \
    get_current_release


class PacingReportFlightsTestCase(APITestCase):
    @staticmethod
    def _get_url(*args):
        return reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.FLIGHTS,
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
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3),
        )
        placement = OpPlacement.objects.create(
            id="2", name="Where is my money", opportunity=opportunity,
            start=today - timedelta(days=2), end=today + timedelta(days=2),
        )
        flight = Flight.objects.create(
            id="3", placement=placement, name="F name", total_cost=200,
            start="2017-01-01", end="2017-12-31", ordered_units=10,
        )

        url = self._get_url(placement.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        item = data[0]
        self.assertEqual(
            set(item.keys()),
            {
                "id", "name", "start", "end", 'is_upcoming', 'is_completed',

                "pacing", "pacing_quality", "pacing_direction",
                "margin", "margin_quality", "margin_direction",
                "video_view_rate_quality", "ctr_quality",

                "plan_video_views", "plan_impressions",
                "plan_cpm", "plan_cpv", "goal_type",

                "plan_cost", "cost",

                "cpv", "cpm", "impressions", "video_views",
                "video_view_rate", "ctr",

                "targeting", "yesterday_budget", "today_goal",
                "yesterday_delivered", "today_budget",
                "before_yesterday_budget",
                "charts",
                "today_goal_views",
                "before_yesterday_delivered_views",
                "yesterday_delivered_views",
                "today_goal_impressions",
                "before_yesterday_delivered_impressions",
                "yesterday_delivered_impressions",
                "tech_fee", "goal_type_id", "dynamic_placement",
                "current_cost_limit"
            }
        )
        flight.refresh_from_db()
        self.assertEqual(item['id'], flight.id)
        self.assertEqual(item['name'], flight.name)
        self.assertEqual(item['start'], flight.start)
        self.assertEqual(item['end'], flight.end)

    def test_hard_cost_placement(self):
        opportunity = Opportunity.objects.create(probability=50)
        amount_spend = 123
        plan_cost = 4325
        placement = OpPlacement.objects.create(
            id="1",
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(id="1",
                              placement=placement,
                              total_cost=plan_cost,
                              ordered_units=1,
                              start=date(2017, 1, 1),
                              end=date(2017, 2, 1))
        campaign = Campaign.objects.create(id="1",
                                           salesforce_placement=placement)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign,
                                         cost=amount_spend)
        url = self._get_url(placement.id)
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        flight_data = response.data[0]
        none_properties = ("pacing", "ctr", "video_view_rate", "cpm", "cpv",
                           "plan_cpm", "plan_cpv", "today_budget", "today_goal",
                           "plan_impressions", "plan_video_views")
        for key in none_properties:
            self.assertIsNone(flight_data[key],
                              "'{key}' should be None".format(key=key))
        self.assertEqual(flight_data["impressions"], 0)
        self.assertEqual(flight_data["video_views"], 0)
        self.assertEqual(flight_data["cost"], amount_spend)
        self.assertEqual(flight_data["plan_cost"], plan_cost)

    def test_hard_cost_placement_margin_zero_cost(self):
        opportunity = Opportunity.objects.create(probability=50)
        placement = OpPlacement.objects.create(
            id="1",
            opportunity=opportunity,
            total_cost=1,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(placement=placement,
                              ordered_units=1,
                              total_cost=1,
                              start=date(2017, 1, 1),
                              end=date(2017, 2, 1))
        Campaign.objects.create(salesforce_placement=placement,
                                cost=0)

        url = self._get_url(placement.id)
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        flight_data = response.data[0]
        self.assertEqual(flight_data["margin"], 100)

    def test_hard_cost_placement_margin_zero_client_cost(self):
        opportunity = Opportunity.objects.create(probability=50)
        placement = OpPlacement.objects.create(
            id="1",
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(placement=placement,
                              ordered_units=1,
                              start=date(2017, 1, 1),
                              end=date(2017, 2, 1))
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=date(2017, 1, 1),
                                         cost=1)

        url = self._get_url(placement.id)
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        flight_data = response.data[0]
        self.assertEqual(flight_data["margin"], -100)

    def test_hard_cost_placement_margin(self):
        opportunity = Opportunity.objects.create(probability=50)
        cost = 134
        client_cost = 654
        expected_margin = (client_cost - cost) * 1. / client_cost * 100
        placement = OpPlacement.objects.create(
            id="1",
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(placement=placement,
                              ordered_units=1,
                              total_cost=client_cost,
                              start=date(2017, 1, 1),
                              end=date(2017, 2, 1))
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=date(2017, 1, 1),
                                         cost=cost)

        url = self._get_url(placement.id)
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        flight_data = response.data[0]
        self.assertAlmostEqual(flight_data["margin"], expected_margin)

    def test_hard_cost_placement_views_goal_and_pacing(self):
        opportunity = Opportunity.objects.create(probability=50)
        placement = OpPlacement.objects.create(
            id="1",
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(placement=placement,
                              ordered_units=1,
                              total_cost=1,
                              start=date(2017, 1, 1),
                              end=date(2017, 2, 1))
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=date(2017, 1, 1),
                                         cost=1)

        url = self._get_url(placement.id)
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        flight_data = response.data[0]
        self.assertIsNone(flight_data["charts"])

    def test_hard_cost_flight_today_budget_and_today_goal(self):
        opportunity = Opportunity.objects.create(probability=50)
        placement = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(
            placement=placement, ordered_units=1, total_cost=1,
            start=date(2017, 1, 1), end=date(2017, 2, 1))
        url = self._get_url(placement.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNone(response.data[0]["today_budget"])
        self.assertIsNone(response.data[0]["today_goal"])
        self.assertIsNone(response.data[0]["yesterday_delivered"])

    def test_pacing_report_flights_charts(self):
        now = datetime(2017, 1, 1)
        start, end = now.date(), date(2017, 1, 31)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end,
        )
        placement = OpPlacement.objects.create(
            id="2", name="Where is my money", opportunity=opportunity,
            start=start, end=end,
            goal_type_id=SalesForceGoalType.CPV
        )
        ordered_unit = 1000
        Flight.objects.create(
            id="3", placement=placement, name="F name", total_cost=200,
            start=start, end=end, ordered_units=ordered_unit,
        )

        url = self._get_url(placement.id)
        with patch_now(now):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        charts = data[0]["charts"]
        self.assertEqual(charts[0]["title"], "Ideal Pacing")
        chart_data = charts[0]["data"]
        chart_values = [i["value"] for i in chart_data]
        plan_units = ordered_unit * 1.02
        days = (end - start).days + 1
        step = plan_units / days
        expected_chart = [step * (i + 1) for i in range(days)]
        self.assertEqual(len(chart_values), len(expected_chart))
        for index, pair in enumerate(zip(chart_values, expected_chart)):
            actual, expected = pair
            self.assertAlmostEqual(actual, expected,
                                   msg="chart value for {index} day is wrong"
                                       "".format(index=index))

    def test_pacing_report_dynamic_placement_plan_stats(self):
        start_1, end_1 = date(2017, 1, 1), date(2017, 1, 31)
        start_2, end_2 = date(2017, 2, 1), date(2017, 3, 31)
        now = datetime.combine(end_2, datetime.min.time())
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start_1, end=end_1,
        )
        placement = OpPlacement.objects.create(
            id="1", name="Where is my money", opportunity=opportunity,
            start=start_1, end=end_2,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=1234
        )
        campaign = Campaign.objects.create(salesforce_placement=placement)
        daily_cost = 10
        daily_views = 40
        daily_impressions = 200
        daily_clicks = 3
        for i in range((end_2 - start_1).days):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start_1 + timedelta(days=i),
                                             cost=daily_cost,
                                             video_views=daily_views + i,
                                             impressions=daily_impressions,
                                             clicks=daily_clicks)
        ordered_unit = 1000
        flight_1 = Flight.objects.create(
            id="1", placement=placement, name="F name", total_cost=200,
            start=start_1, end=end_1, ordered_units=ordered_unit,
        )
        Flight.objects.create(
            id="2", placement=placement, name="F name", total_cost=300,
            start=start_2, end=end_2, ordered_units=ordered_unit,
        )
        flight_1_statistic = CampaignStatistic.objects.filter(
            campaign=campaign, date__gte=start_1, date__lte=end_1) \
            .aggregate(cost=Sum("cost"), views=Sum("video_views"),
                       impressions=Sum("impressions"), clicks=Sum("clicks"))
        cost = flight_1_statistic["cost"]

        url = self._get_url(placement.id)
        with patch_now(now):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        flight = [f for f in response.data if f["id"] == flight_1.id][0]
        self.assertIsNone(flight["plan_video_views"])
        self.assertIsNone(flight["plan_impressions"])
        self.assertIsNone(flight["plan_cpv"])
        self.assertIsNone(flight["plan_cpm"])
        self.assertEqual(flight["plan_cost"], flight_1.total_cost)
        self.assertEqual(flight["cost"], cost)

    def test_pacing_report_dynamic_placement_statistic(self):
        start_1, end_1 = date(2017, 1, 1), date(2017, 1, 31)
        start_2, end_2 = date(2017, 2, 1), date(2017, 3, 31)
        now = datetime.combine(end_2, datetime.min.time())
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start_1, end=end_1,
        )
        placement = OpPlacement.objects.create(
            id="1", name="Where is my money", opportunity=opportunity,
            start=start_1, end=end_2,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=1234
        )
        campaign = Campaign.objects.create(salesforce_placement=placement,
                                           video_views=1)
        daily_cost = 10
        daily_views = 40
        daily_impressions = 200
        daily_clicks = 3
        for i in range((end_2 - start_1).days):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start_1 + timedelta(days=i),
                                             cost=daily_cost,
                                             video_views=daily_views + i,
                                             impressions=daily_impressions,
                                             clicks=daily_clicks)
        ordered_unit = 1000
        flight_1 = Flight.objects.create(
            id="1", placement=placement, name="F name", total_cost=200,
            start=start_1, end=end_1, ordered_units=ordered_unit,
        )
        Flight.objects.create(
            id="2", placement=placement, name="F name", total_cost=200,
            start=start_2, end=end_2, ordered_units=ordered_unit,
        )
        flight_1_statistic = CampaignStatistic.objects.filter(
            campaign=campaign, date__gte=start_1, date__lte=end_1) \
            .aggregate(cost=Sum("cost"), views=Sum("video_views"),
                       impressions=Sum("impressions"), clicks=Sum("clicks"))
        cost = flight_1_statistic["cost"]
        views = flight_1_statistic["views"]
        impressions = flight_1_statistic["impressions"]
        clicks = flight_1_statistic["clicks"]

        url = self._get_url(placement.id)
        with patch_now(now):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        flight = [f for f in response.data if f["id"] == flight_1.id][0]
        self.assertEqual(flight["video_views"], views)
        self.assertEqual(flight["cpv"], cost / views)
        self.assertEqual(flight["cpm"], cost / impressions * 1000)
        self.assertAlmostEqual(flight["video_view_rate"],
                               views / impressions * 100)
        self.assertAlmostEqual(flight["ctr"], clicks / views * 100)

        self.assertTrue(cost > flight_1.total_cost)
        expected_margin = (1 - cost / flight_1.total_cost) * 100.
        expected_pacing = cost / flight_1.total_cost * 100.
        self.assertEqual(flight["margin"], expected_margin)
        self.assertEqual(flight["pacing"], expected_pacing)

    def test_pacing_report_dynamic_placement_daily_statistic(self):
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

        url = self._get_url(placement.id)
        response = self.client.get(url)

        placement_data = response.data[0]
        expected_today_budget = (total_cost - total_spend) / days_left
        self.assertEqual(placement_data["yesterday_budget"], yesterday_spent)
        self.assertEqual(placement_data["today_budget"], expected_today_budget)

    def test_dynamic_placement_budget_charts_ideal_pacing(self):
        today = date(2017, 1, 15)
        start = today - timedelta(days=1)
        end = today + timedelta(days=1)
        total_cost = 12

        expected_ideal_pacing = [
            dict(value=4, label=start),  # yesterday
            dict(value=8, label=today),  # today
            dict(value=12, label=end),  # tomorrow
        ]
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_units=9999,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement,
                              ordered_units=9999,
                              start=start,
                              end=end,
                              total_cost=total_cost)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign, date=start, cost=4,
                                         video_views=8888)
        url = self._get_url(placement.id)
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

    def test_dynamic_placement_budget_charts_ideal_pacing_no_delivery(self):
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
            goal_type_id=SalesForceGoalType.CPV,
            ordered_units=9999,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement,
                              ordered_units=9999,
                              start=start,
                              end=end,
                              total_cost=total_cost)
        url = self._get_url(placement.id)
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
            dict(value=8, label=today),  # today
            dict(value=12, label=end),  # tomorrow
        ]
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_units=9999,
            dynamic_placement=DynamicPlacementType.SERVICE_FEE,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement,
                              ordered_units=9999,
                              start=start,
                              end=end,
                              total_cost=total_cost)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign, date=start, cost=4,
                                         video_views=8888)
        url = self._get_url(placement.id)
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

    def test_dynamic_placement_service_fee_charts_ideal_pacing_no_delivery(
            self):
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
            goal_type_id=SalesForceGoalType.CPV,
            ordered_units=9999,
            dynamic_placement=DynamicPlacementType.SERVICE_FEE,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement, ordered_units=9999,
                              start=start,
                              end=end,
                              total_cost=total_cost)
        url = self._get_url(placement.id)
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

    def test_dynamic_placement_rate_and_tech_fee_charts_ideal_pacing(self):
        today = date(2017, 1, 15)
        start = today - timedelta(days=1)
        end = today + timedelta(days=2)
        total_cost = 12

        expected_ideal_pacing = [
            dict(value=3, label=start),
            dict(value=8, label=today),
            dict(value=10, label=today + timedelta(days=1)),
            dict(value=12, label=end),
        ]
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_units=9999,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement,
                              ordered_units=9999,
                              start=start,
                              end=end,
                              total_cost=total_cost)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign, date=start,
                                         cost=6,
                                         video_views=8888)
        url = self._get_url(placement.id)
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

    def test_dynamic_placement_rate_and_tech_fee_charts_ideal_pacing_no_delivery(
            self):
        today = date(2017, 1, 15)
        start = today - timedelta(days=1)
        end = today + timedelta(days=1)
        total_cost = 12

        expected_ideal_pacing = [
            dict(value=4, label=start),
            dict(value=6, label=today),
            dict(value=12, label=end),
        ]
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start - timedelta(days=1), end=end,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_units=9999,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            total_cost=total_cost,
        )
        Flight.objects.create(id="1", placement=placement,
                              ordered_units=9999, start=start,
                              end=end,
                              total_cost=total_cost)
        url = self._get_url(placement.id)
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
        url = self._get_url(placement.id)
        with patch_now(today):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        flight_data = response.data[0]
        self.assertEqual(flight_data["yesterday_budget"], yesterday_spend)
        self.assertEqual(flight_data["today_budget"], today_goal)

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
        url = self._get_url(placement.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        pl = response.data[0]
        self.assertEqual(pl["plan_cost"], placement.total_cost)

    def test_dynamic_placement_rate_and_tech_fee(self):
        today = date(2017, 1, 1)
        yesterday = today - timedelta(days=1)
        start = today - timedelta(days=3)
        end = today + timedelta(days=5)
        days_total = (end - start).days + 1
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
        plan_budget = total_cost / days_total * days_passed
        expected_pacing = aw_cost * 100. / plan_budget
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
        CampaignStatistic.objects.create(date=yesterday,
                                         campaign=campaign,
                                         cost=aw_cost,
                                         clicks=clicks,
                                         video_views=views,
                                         impressions=impressions)
        url = self._get_url(placement.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        fl = response.data[0]
        self.assertEqual(fl["dynamic_placement"],
                         DynamicPlacementType.RATE_AND_TECH_FEE)
        self.assertAlmostEqual(fl["tech_fee"], tech_fee)
        # contracted rate
        self.assertIsNone(fl["plan_cpv"])
        self.assertIsNone(fl["plan_cpm"])
        # client budget
        self.assertEqual(fl["plan_cost"], total_cost)
        # amount spent
        self.assertEqual(fl["cost"], aw_cost)
        # view rate
        self.assertEqual(fl["video_view_rate"], video_view_rate)

        self.assertEqual(fl["ctr"], ctr)
        self.assertEqual(fl["cpv"], aw_cpv)
        self.assertEqual(fl["cpm"], aw_cpm)
        self.assertEqual(fl["impressions"], impressions)
        self.assertEqual(fl["video_views"], views)
        self.assertAlmostEqual(fl["margin"], expected_margin)
        self.assertAlmostEqual(fl["pacing"], expected_pacing)

    def test_dynamic_placement_rate_and_tech_fee_daily_chart(self):
        today = date(2017, 1, 1)
        yesterday = today - timedelta(days=1)
        start = today - timedelta(days=3)
        end = today + timedelta(days=5)
        days_left = (end - today).days + 1
        tech_fee = 0.12
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )
        total_cost = 123
        aw_cost = 23
        clicks, views, impressions = 3, 14, 164
        aw_cpv = aw_cost * 1. / views
        budget_left = total_cost - (aw_cpv + tech_fee) * views
        spend_kf = aw_cpv / (aw_cpv + tech_fee)
        rate = 2.3
        plan_budget = spend_kf * budget_left / days_left
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
        CampaignStatistic.objects.create(date=yesterday,
                                         campaign=campaign,
                                         cost=aw_cost,
                                         clicks=clicks,
                                         video_views=views,
                                         impressions=impressions)
        url = self._get_url(placement.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        fl = response.data[0]
        self.assertEqual(fl["today_goal"], 0)
        self.assertEqual(fl["today_budget"], plan_budget)
        self.assertEqual(fl["yesterday_delivered"], views)
        self.assertEqual(fl["yesterday_budget"], aw_cost)

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
        url = self._get_url(placement.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        fl = response.data[0]
        ideal_pacing_chart = dict((c["id"], c["data"])
                                  for c in fl["charts"]) \
            .get(PacingReportChartId.IDEAL_PACING, [])
        self.assertEqual(len(ideal_pacing_chart), duration)

        self.assertEqual(fl["dynamic_placement"],
                         DynamicPlacementType.RATE_AND_TECH_FEE)
        self.assertIsNone(fl["plan_video_views"])
        self.assertEqual(fl["today_budget"], today_goal)

        self.assertIsNotNone(fl["charts"])
        charts = dict((c["id"], c["data"]) for c in fl["charts"])
        ideal_pacing = charts.get(PacingReportChartId.IDEAL_PACING, [])
        pacing_values = [c["value"] for c in ideal_pacing]
        expected_chart = [(i + 1) * daily_goal for i in range(duration)]
        for actual, expected in zip(pacing_values, expected_chart):
            self.assertAlmostEqual(actual, expected)

    def test_dynamic_placement_have_no_contacted_rate(self):
        today = date(2017, 1, 1)
        start = today
        end = today + timedelta(days=8)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end
        )

        def create_placement_and_flight(uid, dynamic_placement, gt_id):
            pl = OpPlacement.objects.create(
                id=uid, opportunity=opportunity,
                start=start, end=end, total_cost=12,
                goal_type_id=gt_id,
                ordered_rate=2.1,
                dynamic_placement=dynamic_placement,
                tech_fee=0.02
            )
            Flight.objects.create(id=uid, placement=pl,
                                  start=start,
                                  end=end,
                                  total_cost=12)
            return pl

        test_data = product(
            (
                DynamicPlacementType.BUDGET,
                DynamicPlacementType.SERVICE_FEE,
                DynamicPlacementType.RATE_AND_TECH_FEE
            ),
            (
                SalesForceGoalType.CPV,
                SalesForceGoalType.CPM
            )
        )

        for i, data in enumerate(test_data):
            dynamic_type, goal_type_id = data
            placement = create_placement_and_flight(i, dynamic_type,
                                                    goal_type_id)

            url = self._get_url(placement.id)
            with patch_now(today):
                response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data), 1)
            fl = response.data[0]
            self.assertIsNone(fl["plan_cpm"],
                              "plan_cpm, {}, {}".format(
                                  goal_type_str(goal_type_id),
                                  dynamic_type))
            self.assertIsNone(fl["plan_cpv"],
                              "plan_cpv, {}, {}".format(
                                  goal_type_str(goal_type_id),
                                  dynamic_type))

    def test_dynamic_placement_budget_margin_pacing(self):
        today = date(2017, 1, 15)
        yesterday = today - timedelta(days=1)
        start, end = today - timedelta(days=10), today + timedelta(days=15)
        total_cost = 4322
        total_days = (end - start).days + 1
        days_left = (yesterday - start).days + 1
        aw_cost = 1234
        expected_pacing = aw_cost / (total_cost / total_days * days_left) * 100.
        opportunity = Opportunity.objects.create(probability=100)
        placement = OpPlacement.objects.create(
            id="1",
            opportunity=opportunity,
            total_cost=total_cost,
            dynamic_placement=DynamicPlacementType.BUDGET)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        Flight.objects.create(placement=placement,
                              total_cost=total_cost,
                              start=start, end=end)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=start,
                                         cost=aw_cost)

        url = self._get_url(placement.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        fl_data = response.data[0]
        self.assertEqual(fl_data["margin"], 0)
        self.assertAlmostEqual(fl_data["pacing"], expected_pacing)

    def test_hard_cost_margin_start(self):
        today = date(2018, 1, 1)
        total_cost = 6543
        our_cost = 1234
        start = today - timedelta(days=1)
        end = today + timedelta(days=1)
        self.assertGreater(today, start)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3))
        hard_cost_placement = OpPlacement.objects.create(
            id="2", name="Hard cost placement", opportunity=opportunity,
            start=start, end=end,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(
            start=start, end=end, total_cost=total_cost,
            placement=hard_cost_placement, cost=1)
        Flight.objects.create(id=2,
                              start=today + timedelta(days=1),
                              end=today + timedelta(days=1),
                              total_cost=999999,
                              placement=hard_cost_placement, cost=999)
        campaign = Campaign.objects.create(
            salesforce_placement=hard_cost_placement)
        CampaignStatistic.objects.create(date=start, campaign=campaign,
                                         cost=our_cost)
        client_cost = total_cost
        expected_margin = (1 - our_cost / client_cost) * 100
        url = self._get_url(hard_cost_placement.id)
        with patch_now(today):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data[0]["margin"], expected_margin)
