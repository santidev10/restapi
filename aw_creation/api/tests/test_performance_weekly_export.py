from django.core.urlresolvers import reverse
from django.http import HttpResponse
from rest_framework.status import HTTP_200_OK
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account
from aw_creation.models import AccountCreation
from utils.utils_tests import ExtendedAPITestCase


class AnalyzeExportAPITestCase(ExtendedAPITestCase):

    def test_success(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user, is_managed=False, account=account)

        url = reverse("aw_creation_urls:performance_export_weekly_report",
                      args=(account_creation.id,))
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(type(response), HttpResponse)

    def test_success_demo(self):
        self.create_test_user()
        url = reverse("aw_creation_urls:performance_export_weekly_report",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(type(response), HttpResponse)
