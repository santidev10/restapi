import json

from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED, \
    HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST

from aw_reporting.models import Opportunity, OpPlacement, Campaign, Account, \
    Category, User
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase


class PacingReportOpportunityBufferTestCase(APITestCase):

    def setUp(self):
        self.user = self.create_test_user()

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