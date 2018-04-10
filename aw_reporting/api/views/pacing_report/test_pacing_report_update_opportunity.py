import json

from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED, \
    HTTP_404_NOT_FOUND

from aw_reporting.models import Opportunity, OpPlacement, Campaign, Account, \
    Category, User
from aw_reporting.settings import InstanceSettings
from utils.utils_tests import ExtendedAPITestCase as APITestCase


class PacingReportTestCase(APITestCase):

    def setUp(self):
        self.user = self.create_test_user()

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

        category1 = Category.objects.create(id='Automotive')
        category2 = Category.objects.create(id='Bicycle')

        opportunity = Opportunity.objects.create(
            id="1", name="", category=category1, region_id=0, notes="Hi there",
            account_manager_id="0", sales_manager_id="1", ad_ops_manager_id="2",
        )
        update = dict(
            region=1,
            category=category2.id,
            ad_ops="1",
            am="2",
            sales="0",
            notes="PenPineappleApplePen",
        )

        url = reverse("aw_reporting_urls:pacing_report_update_opportunity",
                      args=(opportunity.id,))
        response = self.client.put(url, json.dumps(update),
                                   content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)
        update['thumbnail'] = "my_image.jpg"
        for k, v in response.data.items():
            self.assertEqual(update[k], v)

    def test_visibility_update(self):
        InstanceSettings().update(global_account_visibility=True)
        opportunity = Opportunity.objects.create(id="1", name="")
        placement = OpPlacement.objects.create(
            id=1, name="", opportunity=opportunity
        )
        account = Account.objects.create(id="11", name="")
        Campaign.objects.create(id=1, name="", account=account,
                                salesforce_placement=placement)

        url = reverse("aw_reporting_urls:pacing_report_update_opportunity",
                      args=(opportunity.id,))
        response = self.client.put(url, dict(region=1))
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        InstanceSettings().update(visible_accounts=["11"])
        response = self.client.patch(url, dict(region=1))
        self.assertEqual(response.status_code, HTTP_200_OK)
