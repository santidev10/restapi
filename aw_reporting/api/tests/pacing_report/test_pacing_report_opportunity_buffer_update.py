import json
from datetime import timedelta

from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_404_NOT_FOUND
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase

from aw_reporting.api.urls.names import Name
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import Opportunity
from aw_reporting.models import OpPlacement
from aw_reporting.models import Flight
from aw_reporting.reports.pacing_report import PacingReport
from saas.urls.namespaces import Namespace


class PacingReportOpportunityBufferTestCase(APITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.pacing_report = PacingReport()

    def test_fail_access_update(self):
        self.user.delete()
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=("0",))
        response = self.client.put(url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_not_found_update(self):
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=("0",))
        response = self.client.put(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_fail_update(self):
        opportunity = Opportunity.objects.create(id="1", name="")
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=(opportunity.id,))
        update = dict(
            cpm_buffer=1,
            cpv_buffer=2,
            name="Not allowed"
        )
        with self.patch_user_settings(global_account_visibility=False):
            response = self.client.put(url, json.dumps(update),
                                       content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_update(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", number="1", probability=100, name="opportunity",
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        update = dict(
            cpm_buffer=1,
            cpv_buffer=2
        )
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=(opportunity.id,))
        response = self.client.put(url, json.dumps(update), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_response_cpm_default_buffers_goal_factor(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100, budget=10)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        flight = Flight.objects.create(
            id="1", placement=placement, name="F",
            start=now - timedelta(days=1), end=now + timedelta(days=1), ordered_units=100
        )
        url = "{}?search={}".format(reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.OPPORTUNITIES), opportunity.name)
        response = self.client.get(url)
        cpm_buffer = self.pacing_report.goal_factor
        expected_plan_impressions = flight.ordered_units * cpm_buffer
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data["items"][0]
        self.assertEqual(data["plan_impressions"], expected_plan_impressions)
        self.assertEqual(data["plan_video_views"], None)
        self.assertAlmostEqual(data["cpm_buffer"], (self.pacing_report.goal_factor - 1) * 100)
        self.assertAlmostEqual(data["cpv_buffer"], (self.pacing_report.goal_factor - 1) * 100)

    def test_success_response_cpm_default_buffers_big_goal_factor(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100, budget=1000000)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        flight = Flight.objects.create(
            id="1", placement=placement, name="F",
            start=now - timedelta(days=1), end=now + timedelta(days=1), ordered_units=100
        )
        url = "{}?search={}".format(reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.OPPORTUNITIES), opportunity.name)
        response = self.client.get(url)
        cpm_buffer = self.pacing_report.big_goal_factor
        expected_plan_impressions = flight.ordered_units * cpm_buffer
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data["items"][0]
        self.assertEqual(data["plan_impressions"], expected_plan_impressions)
        self.assertEqual(data["plan_video_views"], None)
        self.assertAlmostEqual(data["cpm_buffer"], (self.pacing_report.big_goal_factor - 1) * 100)
        self.assertAlmostEqual(data["cpv_buffer"], (self.pacing_report.big_goal_factor - 1) * 100)

    def test_success_response_cpv_default_buffers_goal_factor(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100, budget=10)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        flight = Flight.objects.create(
            id="1", placement=placement, name="F",
            start=now - timedelta(days=1), end=now + timedelta(days=1), ordered_units=100
        )
        url = "{}?search={}".format(reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.OPPORTUNITIES), opportunity.name)
        response = self.client.get(url)
        cpv_buffer = self.pacing_report.goal_factor
        expected_plan_views = flight.ordered_units * cpv_buffer
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data["items"][0]
        self.assertEqual(data["plan_impressions"], None)
        self.assertEqual(data["plan_video_views"], expected_plan_views)
        self.assertAlmostEqual(data["cpm_buffer"], (self.pacing_report.goal_factor - 1) * 100)
        self.assertAlmostEqual(data["cpv_buffer"], (self.pacing_report.goal_factor - 1) * 100)

    def test_success_response_cpv_default_buffers_big_goal_factor(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100, budget=1000000)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        flight = Flight.objects.create(
            id="1", placement=placement, name="F",
            start=now - timedelta(days=1), end=now + timedelta(days=1), ordered_units=100
        )
        url = "{}?search={}".format(reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.OPPORTUNITIES), opportunity.name)
        response = self.client.get(url)
        cpm_buffer = self.pacing_report.big_goal_factor
        expected_plan_views = flight.ordered_units * cpm_buffer
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data["items"][0]
        self.assertEqual(data["plan_impressions"], None)
        self.assertEqual(data["plan_video_views"], expected_plan_views)
        self.assertAlmostEqual(data["cpm_buffer"], (self.pacing_report.big_goal_factor - 1) * 100)
        self.assertAlmostEqual(data["cpv_buffer"], (self.pacing_report.big_goal_factor - 1) * 100)

    def test_success_response_cpm(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100, budget=10)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        flight = Flight.objects.create(
            id="1", placement=placement, name="F",
            start=now - timedelta(days=1), end=now + timedelta(days=1), ordered_units=100
        )
        update = dict(
            cpm_buffer=10,
        )
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=(opportunity.id,))
        response = self.client.put(url, json.dumps(update), content_type="application/json")
        cpm_buffer = 1 + (update["cpm_buffer"] / 100)
        expected_plan_impressions = flight.ordered_units * cpm_buffer

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["plan_impressions"], expected_plan_impressions)
        self.assertEqual(response.data["plan_video_views"], None)

    def test_success_response_cpv_with_goal_factor(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100, budget=100)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        flight = Flight.objects.create(
            id="1", placement=placement, name="F", total_cost=100,
            start=now - timedelta(days=1), end=now + timedelta(days=1), ordered_units=100, delivered=50
        )
        update = dict(
            cpv_buffer=10,
        )
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=(opportunity.id,))
        response = self.client.put(url, json.dumps(update), content_type="application/json")
        cpv_buffer = 1 + (update["cpv_buffer"] / 100)
        expected_video_views = flight.ordered_units * cpv_buffer

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["plan_impressions"], None)
        self.assertEqual(response.data["plan_video_views"], expected_video_views)

    def test_success_response_cpv_cpm_with_goal_factor(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100, budget=1000)
        placement_cpm = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        placement_cpv = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        flight_cpm = Flight.objects.create(
            id="1", placement=placement_cpm, name="F", total_cost=100,
            start=now - timedelta(days=1), end=now + timedelta(days=1), ordered_units=100, delivered=50
        )
        flight_cpv = Flight.objects.create(
            id="2", placement=placement_cpv, name="F", total_cost=200,
            start=now - timedelta(days=1), end=now + timedelta(days=1), ordered_units=100, delivered=50
        )
        update = dict(
            cpm_buffer=20,
            cpv_buffer=20,
        )
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=(opportunity.id,))
        response = self.client.put(url, json.dumps(update), content_type="application/json")
        cpv_buffer = 1 + (update["cpv_buffer"] / 100)
        cpm_buffer = 1 + (update["cpm_buffer"] / 100)

        expected_video_views = flight_cpv.ordered_units * cpv_buffer
        expected_plan_impressions = flight_cpm.ordered_units * cpm_buffer

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["plan_impressions"], expected_plan_impressions)
        self.assertEqual(response.data["plan_video_views"], expected_video_views)
