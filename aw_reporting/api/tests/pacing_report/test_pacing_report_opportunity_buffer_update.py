import json
from datetime import date, datetime, timedelta

from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_404_NOT_FOUND
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase
from utils.utittests.patch_now import patch_now

from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import Opportunity
from aw_reporting.models import OpPlacement
from aw_reporting.models import Flight
from aw_reporting.models import User
from aw_reporting.reports.pacing_report import PacingReport, apply_buffer


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
            name='Not allowed'
        )
        with self.patch_user_settings(global_account_visibility=False):
            response = self.client.put(url, json.dumps(update),
                                       content_type='application/json')
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
        response = self.client.put(url, json.dumps(update), content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_response_cpm(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100,)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            start=now - timedelta(days=1), end=now + timedelta(days=1),
        )
        flight = Flight.objects.create(
            id="1", placement=placement, name="F", total_cost=100,
            start=now - timedelta(days=1), end=now + timedelta(days=1), ordered_units=100, delivered=50
        )
        update = dict(
            cpm_buffer=10,
        )
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=(opportunity.id,))
        response = self.client.put(url, json.dumps(update), content_type='application/json')
        expected_plan_impressions = self.pacing_report.goal_factor * flight.ordered_units * (1 + update['cpm_buffer'] / 100)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['plan_impressions'], expected_plan_impressions)
        self.assertEqual(response.data['plan_video_views'], None)

    def test_success_response_cpv(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100, )
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
        response = self.client.put(url, json.dumps(update), content_type='application/json')
        expected_video_views = self.pacing_report.goal_factor * flight.ordered_units * (1 + update['cpv_buffer'] / 100)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['plan_impressions'], None)
        self.assertEqual(response.data['plan_video_views'], expected_video_views)

    def test_success_response_cpv(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="opportunity", start=now - timedelta(days=1),
            end=now + timedelta(days=1), probability=100, )
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
            cpm_buffer=10,
            cpv_buffer=10,
        )
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=(opportunity.id,))
        response = self.client.put(url, json.dumps(update), content_type='application/json')
        expected_plan_impressions = self.pacing_report.goal_factor * flight_cpm.ordered_units * (1 + update['cpm_buffer'] / 100)
        expected_video_views = self.pacing_report.goal_factor * flight_cpv.ordered_units * (1 + update['cpv_buffer'] / 100)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['plan_impressions'], expected_plan_impressions)
        self.assertEqual(response.data['plan_video_views'], expected_video_views)

    def test_pacing_report_flights_charts_with_opportunity_buffers(self):
        now = datetime(2017, 1, 1)
        start, end = now.date(), date(2017, 1, 31)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end, cpm_buffer=10, cpv_buffer=10,
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
        url = reverse("aw_reporting_urls:pacing_report_flights", args=(placement.id,))
        with patch_now(now):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        charts = data[0]["charts"]
        self.assertEqual(charts[0]["title"], "Ideal Pacing")
        chart_data = charts[0]["data"]
        chart_values = [i["value"] for i in chart_data]
        plan_units = apply_buffer(ordered_unit * 1.02, opportunity.cpv_buffer)
        days = (end - start).days + 1
        step = plan_units / days
        expected_chart = [step * (i + 1) for i in range(days)]
        self.assertEqual(len(chart_values), len(expected_chart))
        for index, pair in enumerate(zip(chart_values, expected_chart)):
            actual, expected = pair
            self.assertAlmostEqual(actual, expected,
                                   msg="chart value for {index} day is wrong"
                                       "".format(index=index))