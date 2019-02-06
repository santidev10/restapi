import json

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_401_UNAUTHORIZED, \
    HTTP_400_BAD_REQUEST, HTTP_202_ACCEPTED

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Opportunity, OpPlacement, Flight, Campaign, Account
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase


class PacingReportFlightCampaignAllocationsTestCase(ExtendedAPITestCase):
    def _update(self, flight_id, data):
        url = reverse(
            "{namespace}:{viewname}".format(
                namespace=Namespace.AW_REPORTING,
                viewname=Name.PacingReport.FLIGHTS_CAMPAIGN_ALLOCATIONS),
            kwargs=dict(pk=str(flight_id)))
        return self.client.put(url, json.dumps(data),
                               content_type="application/json")

    def test_auth_required(self):
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)

        put_data = dict()
        response = self._update(flight.id, put_data)

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_bad_request_on_empty_campaign_set(self):
        self.create_test_user()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)

        put_data = dict()
        response = self._update(flight.id, put_data)

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_bad_request_on_wrong_campaign_set(self):
        self.create_test_user()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        Campaign.objects.create(id=1, salesforce_placement=placement)
        irrelevant_campaign = Campaign.objects.create(id=2)

        put_data = {irrelevant_campaign.id: 100}
        response = self._update(flight.id, put_data)

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_bad_request_on_wrong_percentage(self):
        self.create_test_user()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement)
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement)

        put_data = {campaign_1.id: 10,
                    campaign_2.id: 10}
        self.assertNotEqual(sum(put_data.values()), 100)

        response = self._update(flight.id, put_data)

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success(self):
        self.create_test_user()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        flight = Flight.objects.create(id=1, placement=placement)
        account = Account.objects.create(id=1)

        campaign_1 = Campaign.objects.create(
            id=1, salesforce_placement=placement, account=account)
        campaign_2 = Campaign.objects.create(
            id=2, salesforce_placement=placement, account=account)

        allocation_1, allocation_2 = 70, 30
        put_data = {campaign_1.id: allocation_1,
                    campaign_2.id: allocation_2}
        self.assertEqual(sum(put_data.values()), 100)

        response = self._update(flight.id, put_data)
        campaign_1.refresh_from_db()
        campaign_2.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        self.assertEqual(campaign_1.goal_allocation, allocation_1)
        self.assertEqual(campaign_2.goal_allocation, allocation_2)
