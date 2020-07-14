import json
from datetime import datetime

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignBudgetHistory
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class PacingReportStatus(ExtendedAPITestCase):
    @staticmethod
    def _get_url(*args):
        return reverse(Name.PacingReport.PACING_REPORT_STATUS, [Namespace.AW_REPORTING],
                       args=args)

    def setUp(self):
        self.create_test_user()
        c1 = Campaign.objects.create(id="1")
        c2 = Campaign.objects.create(id="2")
        CampaignBudgetHistory.objects.create(campaign=c1, budget=1)
        CampaignBudgetHistory.objects.create(campaign=c2, budget=2)

    def test_success(self):
        url = self._get_url()
        payload = json.dumps({
            "campaignIds": ["1", "2"],
            "budgetHistoryIds": ["1", "2"]
        })
        c1 = Campaign.objects.get(id=1)
        c2 = Campaign.objects.get(id=2)
        h1 = c1.budget_history.first()
        h2 = c2.budget_history.first()
        self.assertIsNone(c1.sync_time)
        self.assertIsNone(c1.sync_time)
        self.assertIsNone(h1.sync_at)
        self.assertIsNone(h2.sync_at)
        response = self.client.patch(url, payload, content_type="application/json")
        c1.refresh_from_db()
        c2.refresh_from_db()
        h1.refresh_from_db()
        h2.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(isinstance(c1.sync_time, datetime))
        self.assertTrue(isinstance(c2.sync_time, datetime))
        self.assertTrue(isinstance(h1.sync_at, datetime))
        self.assertTrue(isinstance(h2.sync_at, datetime))

    def test_fail(self):
        url = self._get_url()
        payload = json.dumps({
            "data": [1, 2]
        })
        response = self.client.patch(url, payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_message(self):
        url = self._get_url()
        payload = json.dumps({
            "campaignIds": ["1", "2"]
        })
        response = self.client.patch(url, payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, "Campaigns sync complete for: 1, 2")
