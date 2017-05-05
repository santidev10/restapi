from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.test import APITestCase
from aw_reporting.demo.models import *
import json


class AccountNamesAPITestCase(APITestCase):

    def test_success_get_filter_dates(self):
        url = reverse("aw_reporting_urls:analyze_chart",
                      args=(DEMO_ACCOUNT_ID,))
        today = datetime.now().date()
        response = self.client.post(
            url,
            json.dumps(dict(start_date=str(today - timedelta(days=2)),
                            end_date=str(today - timedelta(days=1)))),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 3, "Continue on Monday")

