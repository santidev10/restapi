import json

from utils.utittests.reverse import reverse
from rest_framework.status import HTTP_401_UNAUTHORIZED, \
    HTTP_400_BAD_REQUEST, HTTP_202_ACCEPTED, HTTP_200_OK

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Campaign
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase

class PacingReportStatus(ExtendedAPITestCase):
    @staticmethod
    def _get_url(*args):
        return reverse(Name.PacingReport.PACING_REPORT_STATUS, [Namespace.AW_REPORTING],
                       args=args)

    def setUp(self):
        self.create_test_user()
        campaign_1 = Campaign.objects.create(
            id='1')
        campaign_2 = Campaign.objects.create(
            id='2')

    def test_success(self):
        url = self._get_url()
        payload = json.dumps({
            'campaignIds': ['1', '2']
        })
        response = self.client.patch(url, payload, content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_fail(self):
        url = self._get_url()
        payload = json.dumps({
            'data': [1, 2]
        })
        response = self.client.patch(url, payload, content_type='application/json')
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_message(self):
        url = self._get_url()
        payload = json.dumps({
            'campaignIds': ['1', '2']
        })
        response = self.client.patch(url, payload, content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, 'Campaigns sync complete for: 1, 2')
