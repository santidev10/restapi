import json
from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_404_NOT_FOUND
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import Opportunity
from aw_reporting.models import OpPlacement
from aw_reporting.models import Flight
from aw_reporting.models import User
from aw_reporting.reports.pacing_report import PacingReport
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase
from datetime import timedelta


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
        User.objects.bulk_create([
            User(id=str(i), name="Slave #%d" % i, email="profile%d@mail.kz" % i)
            for i in range(3)
        ])
        get_user_model().objects.create(email="profile1@mail.kz",
                                        profile_image_url="my_image.jpg")

        opportunity = Opportunity.objects.create(id="1", name="")
        update = dict(
            cpm_buffer=1,
            cpv_buffer=2
        )

        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer",
                      args=(opportunity.id,))

        with self.patch_user_settings(global_account_visibility=False):
            response = self.client.put(url, json.dumps(update),
                                       content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_response(self):
        now = timezone.now()
        opportunity = Opportunity.objects.create(id="1", name="")
        placement = OpPlacement.objects.create(id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM)
        flight = Flight.objects.create(
            id="1", placement=placement, name="F", total_cost=100,
            start=now - timedelta(days=5), end=now + timedelta(days=5), ordered_units=100,
        )
        buffer_update = dict(
            cpm_buffer=10,
            cpv_buffer=10
        )
        url = reverse("aw_reporting_urls:pacing_report_opportunity_buffer", args=(opportunity.id,))
        with self.patch_user_settings(global_account_visibility=False):
            response = self.client.put(url, json.dumps(buffer_update),
                                       content_type='application/json')

            pacing_report_goal_factor = self.pacing_report.goal_factor
            placement_plan_impressions = flight.ordered_units * pacing_report_goal_factor * (1 + buffer_update['cpm_buffer'] / 100)
            updated_placement = [p for p in response.data if p['id'] == placement.id][0]
            self.assertEqual(updated_placement['plan_impressions'], placement_plan_impressions)