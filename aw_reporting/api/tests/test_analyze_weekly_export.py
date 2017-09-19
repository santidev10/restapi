import json
from django.core.urlresolvers import reverse
from django.http import HttpResponse
from rest_framework.status import HTTP_200_OK
from aw_reporting.demo.models import *
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from .base import AwReportingAPITestCase
from unittest.mock import patch


class AnalyzeExportAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.account = self.create_account(self.user)

    def test_success(self):
        campaign = Campaign.objects.create(id=1, name="", account=self.account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        date = datetime.now()
        AdGroupStatistic.objects.create(
            ad_group=ad_group, date=date, average_position=1,
        )

        url = reverse("aw_reporting_urls:analyze_export_weekly_report",
                      args=(self.account.id,))
        today = datetime.now().date()
        filters = {
            'start_date': str(today - timedelta(days=1)),
            'end_date': str(today),
        }
        response = self.client.post(
            url, json.dumps(filters), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(type(response), HttpResponse)
