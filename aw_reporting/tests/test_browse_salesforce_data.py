from datetime import date
from unittest.mock import patch, MagicMock

from django.core.management import call_command
from django.test import TransactionTestCase

from aw_reporting.models import OpPlacement, Flight, Opportunity, Campaign, \
    CampaignStatistic
from aw_reporting.models.salesforce_constants import DynamicPlacementType, \
    SalesForceGoalType
from aw_reporting.salesforce import Connection
from utils.utittests.patch_now import patch_now


class BrowseSalesforceDataTestCase(TransactionTestCase):
    def test_update_dynamic_placement_service_fee(self):
        opportunity = Opportunity.objects.create(id=1)
        today = start = end = date(2017, 1, 1)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.SERVICE_FEE)
        flight = Flight.objects.create(id=123,
                                       placement=placement,
                                       start=start, end=end)
        campaign = Campaign.objects.create(
            salesforce_placement=placement
        )
        CampaignStatistic.objects.create(
            date=date(2017, 1, 1),
            campaign=campaign,
            cost=999,
            impressions=999,
            video_views=999
        )
        flight.refresh_from_db()

        sf_mock = MagicMock()
        sf_mock().sf.Flight__c.update.return_value = 204
        sf_mock().sf.Placement__c.update.return_value = 204

        with patch("aw_reporting.management.commands"
                   ".browse_salesforce_data.SConnection", new=sf_mock), \
             patch_now(today):
            call_command("browse_salesforce_data", no_get="1")

        sf_mock().sf.Flight__c.update.assert_called_once_with(
            flight.id, dict(Delivered_Ad_Ops__c=1, Total_Flight_Cost__c=0))

    def test_update_dynamic_placement_rate_and_tech_fee(self):
        opportunity = Opportunity.objects.create(id=1)
        today = start = end = date(2017, 1, 1)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE)
        cost, delivered_units = 12, 123
        flight = Flight.objects.create(id=123,
                                       placement=placement,
                                       start=start, end=end)
        campaign = Campaign.objects.create(
            salesforce_placement=placement
        )
        CampaignStatistic.objects.create(
            date=date(2017, 1, 1),
            campaign=campaign,
            cost=cost,
            impressions=999,
            video_views=delivered_units
        )
        flight.refresh_from_db()

        sf_mock = MagicMock()
        sf_mock().sf.Flight__c.update.return_value = 204
        sf_mock().sf.Placement__c.update.return_value = 204

        with patch("aw_reporting.management.commands"
                   ".browse_salesforce_data.SConnection", new=sf_mock), \
             patch_now(today):
            call_command("browse_salesforce_data", no_get="1")

        sf_mock().sf.Flight__c.update.assert_called_once_with(
            flight.id, dict(Delivered_Ad_Ops__c=delivered_units,
                            Total_Flight_Cost__c=cost))

    def test_links_placements_by_code_on_campaign(self):
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity,
                                               number="PL12345")
        campaign = Campaign.objects.create(placement_code=placement.number)
        campaign.refresh_from_db()

        self.assertIsNone(campaign.salesforce_placement)

        call_command("browse_salesforce_data", no_get="1", no_update="1")
        campaign.refresh_from_db()

        self.assertIsNotNone(campaign.salesforce_placement)
        self.assertEqual(campaign.salesforce_placement, placement)

    def test_does_not_brake_links(self):
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        Campaign.objects.create(salesforce_placement=placement)

        call_command("browse_salesforce_data", no_get="1", no_update="1")

        self.assertEqual(placement.adwords_campaigns.count(), 1)

    def test_success_opportunity_create(self):
        self.assertEqual(Opportunity.objects.all().count(), 0)

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("Opportunity", [
            dict(
                Id="123",
                Name="",
                DO_NOT_STRAY_FROM_DELIVERY_SCHEDULE__c=False,
                Probability=100,
                CreatedDate=None,
                CloseDate=None,
                Renewal_Approved__c=False,
                Reason_for_Close_Lost__c=None,
                Demo_TEST__c=None,
                Geo_Targeting_Country_State_City__c=None,
                Targeting_Tactics__c=None,
                Tags__c=None,
                Types_of__c=None,
                APEX_Deal__c=False,
                Bill_off_3p_Numbers__c=False
            )
        ])
        with patch(
                "aw_reporting.management.commands.browse_salesforce_data.SConnection",
                return_value=sf_mock):
            call_command("browse_salesforce_data", no_update="1")

        self.assertEqual(Opportunity.objects.all().count(), 1)


class MockSalesforceConnection(Connection):
    def __init__(self):
        self._storage = dict()

    def add_mocked_items(self, name, items):
        self._storage[name] = self._storage.get(name, []) + items

    def get_items(self, name, fields, where):
        for item in self._storage.get(name, []):
            yield item

    def describe(self, *_):
        return {
            "fields": [{"name": "Client_Vertical__c", "picklistValues": []}]}
