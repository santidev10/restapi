import json

from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import Category
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import User
from aw_reporting.models.salesforce_constants import OpportunityConfig
from aw_reporting.reports.constants import PacingReportPeriod
from userprofile.constants import StaticPermissions
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase


class PacingReportTestCase(APITestCase):

    def setUp(self):
        self.user = self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})

    def test_fail_access_update(self):
        self.user.delete()
        url = reverse("aw_reporting_urls:pacing_report_update_opportunity",
                      args=("0",))
        response = self.client.put(url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_not_found_update(self):
        url = reverse("aw_reporting_urls:pacing_report_update_opportunity",
                      args=("0",))
        response = self.client.put(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success_update(self):
        User.objects.bulk_create([
            User(id=str(i), name="Slave #%d" % i, email="profile%d@mail.kz" % i)
            for i in range(3)
        ])
        get_user_model().objects.create(email="profile1@mail.kz",
                                        profile_image_url="my_image.jpg")

        category1 = Category.objects.create(id="Automotive")
        category2 = Category.objects.create(id="Bicycle")

        opportunity = Opportunity.objects.create(
            id="1", name="", category=category1, territory="test 1", notes="Hi there",
            account_manager_id="0", sales_manager_id="1", ad_ops_manager_id="2",
            config={},
        )
        update = dict(
            region="test 2",
            category=category2.id,
            ad_ops="1",
            am="2",
            sales="0",
            notes="PenPineappleApplePen",
            cpm_buffer=0,
            cpv_buffer=0,
            config={
                OpportunityConfig.MARGIN_PERIOD.value: PacingReportPeriod.MONTH.value,
            },
        )

        url = reverse("aw_reporting_urls:pacing_report_update_opportunity",
                      args=(opportunity.id,))
        response = self.client.put(url, json.dumps(update), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        update["thumbnail"] = "my_image.jpg"
        for k, v in response.data.items():
            self.assertEqual(update[k], v)

    def test_visibility_update(self):
        self.create_test_user(perms={
            StaticPermissions.PACING_REPORT: True,
            StaticPermissions.MANAGED_SERVICE__GLOBAL_ACCOUNT_VISIBILITY: True
        })
        opportunity = Opportunity.objects.create(id="1", name="")
        placement = OpPlacement.objects.create(
            id=1, name="", opportunity=opportunity
        )
        account = Account.objects.create(id="11", name="")
        Campaign.objects.create(id=1, name="", account=account,
                                salesforce_placement=placement)

        url = reverse("aw_reporting_urls:pacing_report_update_opportunity",
                      args=(opportunity.id,))

        with self.patch_user_settings(visible_accounts=[]):
            response = self.client.put(url, dict(region=1))
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        with self.patch_user_settings(visible_accounts=[account.id]):
            response = self.client.patch(url, dict(region=1))
        self.assertEqual(response.status_code, HTTP_200_OK)
