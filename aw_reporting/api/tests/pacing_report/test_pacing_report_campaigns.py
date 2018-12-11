from datetime import timedelta

from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED, \
    HTTP_404_NOT_FOUND

from aw_reporting.models import Opportunity, OpPlacement, Flight, Campaign, \
    CampaignStatistic, SalesForceGoalType
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
