from django.core.urlresolvers import reverse
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK
from rest_framework.test import APITestCase
from aw_reporting.demo.models import *
import json


class AnalyzeExportAPITestCase(APITestCase):

    def test_success(self):
        url = reverse("aw_reporting_urls:analyze_export",
                      args=(DEMO_ACCOUNT_ID,))
        today = datetime.now().date()
        filters = {
            'start_date': str(today - timedelta(days=1)),
            'end_date': str(today),
        }
        response = self.client.post(
            url, json.dumps(filters), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(type(response), StreamingHttpResponse)

