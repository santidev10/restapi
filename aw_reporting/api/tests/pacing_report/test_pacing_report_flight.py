import json

from django.urls import reverse
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class PacingReportTestCase(ExtendedAPITestCase):

    def _get_url(self, flight_id):
        url = reverse(f"aw_reporting_urls:{Name.PacingReport.FLIGHT}", kwargs={"pk": flight_id})
        return url

    def _create_flight(self):
        op = Opportunity.objects.create()
        pl = OpPlacement.objects.create(opportunity=op)
        fl = Flight.objects.create(placement=pl, id=f"id_{next(int_iterator)}")
        return fl

    def test_margin_cap_out_range(self):
        """ Test margin_cap must be between 0 and 100, inclusive """
        self.create_admin_user()
        flight = self._create_flight()
        payload = {
            "margin_cap": "100.1"
        }
        res1 = self.client.patch(self._get_url(flight.id), content_type="application/json", data=json.dumps(payload))
        self.assertEqual(res1.status_code, HTTP_400_BAD_REQUEST)

        payload = {
            "margin_cap": "-0.01"
        }
        res2 = self.client.patch(self._get_url(flight.id), content_type="application/json", data=json.dumps(payload))
        self.assertEqual(res2.status_code, HTTP_400_BAD_REQUEST)