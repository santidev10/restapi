from datetime import datetime
from datetime import timedelta

from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import PacingReportChartId
from userprofile.constants import UserSettingsKey
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase


class PacingReportTestCase(APITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_forbidden_get(self):
        self.user.delete()
        url = reverse("aw_reporting_urls:pacing_report_campaigns", args=(1,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_not_found_get(self):
        url = reverse("aw_reporting_urls:pacing_report_campaigns", args=(1,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success_get(self):
        today = timezone.now()
        start = today - timedelta(days=5)
        end = today + timedelta(days=3)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end,
        )
        placement = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity, start=start, end=end,
            goal_type_id=SalesForceGoalType.CPV,
        )

        flight = Flight.objects.create(
            id="3", placement=placement, name="", start=start, end=end,
            ordered_units=1000, total_cost=100,
        )
        campaign = Campaign.objects.create(
            id="4", name="Special K", salesforce_placement=placement,
            start_date=start, end_date=end,
        )
        for i in range(5):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start + timedelta(days=i),
                                             impressions=1000, video_views=100)

        url = reverse("aw_reporting_urls:pacing_report_campaigns",
                      args=(flight.id,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        item = data[0]
        self.assertEqual(
            set(item.keys()),
            {
                "aw_update_time",
                "charts",
                "cost",
                "cpm",
                "cpv",
                "ctr",
                "ctr_quality",
                "current_cost_limit",
                "end",
                "goal",
                "goal_allocation",
                "goal_type",
                "id",
                "impressions",
                "is_completed",
                "is_upcoming",
                "margin",
                "margin_direction",
                "margin_quality",
                "name",
                "pacing",
                "pacing_direction",
                "pacing_quality",
                "plan_cost",
                "plan_cpm",
                "plan_cpv",
                "plan_impressions",
                "plan_video_views",
                "start",
                "targeting",
                "today_budget",
                "today_goal",
                "today_goal_impressions",
                "today_goal_views",
                "video_view_rate",
                "video_view_rate_quality",
                "video_views",
                "yesterday_budget",
                "yesterday_delivered",
                "yesterday_delivered_impressions",
                "yesterday_delivered_views",
            }
        )
        campaign.refresh_from_db()
        self.assertEqual(item["id"], campaign.id)
        self.assertEqual(item["name"], campaign.name)
        self.assertEqual(item["start"], campaign.start_date)
        self.assertEqual(item["end"], campaign.end_date)

        delivery_chart = item["charts"][1]
        self.assertEqual(delivery_chart["title"], "Daily Deviation")
        self.assertEqual(delivery_chart["data"][-1]["value"], 500)

    def test_campaign_allocation_goal(self):
        now = datetime(2018, 10, 10, 10, 10)
        today = now.date()
        ordered_views = 100
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            ordered_units=ordered_views,
            start=today,
            end=today,
        )
        allocation_1 = 30
        allocation_2 = 100 - allocation_1
        campaign_1 = Campaign.objects.create(
            id=str(next(int_iterator)),
            salesforce_placement=placement,
            goal_allocation=allocation_1,
        )
        campaign_2 = Campaign.objects.create(
            id=str(next(int_iterator)),
            salesforce_placement=placement,
            goal_allocation=allocation_2,
        )

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        url = reverse("aw_reporting_urls:pacing_report_campaigns", args=(flight.id,))
        with self.patch_user_settings(**user_settings), \
             patch_now(now):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 2)
        items_by_id = {item["id"]: item for item in data}
        expected_goal_1 = ordered_views * allocation_1 / 100
        expected_goal_2 = ordered_views * allocation_2 / 100

        chart_data_1 = items_by_id[campaign_1.id]
        chart_data_2 = items_by_id[campaign_2.id]
        self.assertEqual(chart_data_1["goal"], expected_goal_1)
        self.assertEqual(chart_data_2["goal"], expected_goal_2)

    def test_campaign_allocation_planned_delivery_chart(self):
        now = datetime(2018, 10, 10, 10, 10)
        today = now.date()
        ordered_views = 100
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            ordered_units=ordered_views,
            start=today,
            end=today,
        )
        allocation_1 = 30
        allocation_2 = 100 - allocation_1
        campaign_1 = Campaign.objects.create(
            id=str(next(int_iterator)),
            salesforce_placement=placement,
            goal_allocation=allocation_1,
        )
        campaign_2 = Campaign.objects.create(
            id=str(next(int_iterator)),
            salesforce_placement=placement,
            goal_allocation=allocation_2,
        )

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        url = reverse("aw_reporting_urls:pacing_report_campaigns", args=(flight.id,))
        with self.patch_user_settings(**user_settings), \
             patch_now(now):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 2)
        items_by_id = {item["id"]: item for item in data}
        expected_goal_1 = ordered_views * allocation_1 / 100 * PacingReport.goal_factor
        expected_goal_2 = ordered_views * allocation_2 / 100 * PacingReport.goal_factor

        def get_planned_delivery(campaign_id):
            charts = items_by_id[campaign_id]["charts"]
            for chart in charts:
                if chart["id"] == PacingReportChartId.PLANNED_DELIVERY:
                    return chart["data"][-1]["value"]

        self.assertAlmostEqual(get_planned_delivery(campaign_1.id), expected_goal_1)
        self.assertAlmostEqual(get_planned_delivery(campaign_2.id), expected_goal_2)

    def test_campaign_allocation_historical_goal_chart(self):
        now = datetime(2018, 10, 10, 10, 10)
        today = now.date()
        ordered_views = 100
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            ordered_units=ordered_views,
            start=today,
            end=today,
        )
        allocation_1 = 30
        allocation_2 = 100 - allocation_1
        campaign_1 = Campaign.objects.create(
            id=str(next(int_iterator)),
            salesforce_placement=placement,
            goal_allocation=allocation_1,
        )
        campaign_2 = Campaign.objects.create(
            id=str(next(int_iterator)),
            salesforce_placement=placement,
            goal_allocation=allocation_2,
        )

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        url = reverse("aw_reporting_urls:pacing_report_campaigns", args=(flight.id,))
        with self.patch_user_settings(**user_settings), \
             patch_now(now):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 2)
        items_by_id = {item["id"]: item for item in data}
        expected_goal_1 = ordered_views * allocation_1 / 100 * PacingReport.goal_factor
        expected_goal_2 = ordered_views * allocation_2 / 100 * PacingReport.goal_factor

        def get_planned_delivery(campaign_id):
            charts = items_by_id[campaign_id]["charts"]
            for chart in charts:
                if chart["id"] == PacingReportChartId.HISTORICAL_GOAL:
                    return chart["data"][0]["value"]

        self.assertAlmostEqual(get_planned_delivery(campaign_1.id), expected_goal_1)
        self.assertAlmostEqual(get_planned_delivery(campaign_2.id), expected_goal_2)


