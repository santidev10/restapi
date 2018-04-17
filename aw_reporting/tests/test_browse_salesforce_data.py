from datetime import date
from unittest.mock import patch, MagicMock

from django.core.management import call_command

from aw_reporting.models import OpPlacement, Flight, Opportunity, Campaign, \
    CampaignStatistic
from aw_reporting.models.salesforce_constants import DynamicPlacementType, \
    SalesForceGoalType
from utils.utils_tests import ExtendedAPITestCase as APITestCase


class BrowseSalesforceDataTestCase(APITestCase):
    def test_update_dynamic_placement_service_fee(self):
        opportunity = Opportunity.objects.create(id=1)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.SERVICE_FEE)
        flight = Flight.objects.create(id=123,
                                       placement=placement)
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
                   ".browse_salesforce_data.SConnection", new=sf_mock):
            call_command("browse_salesforce_data", no_get="1")

        sf_mock().sf.Flight__c.update.assert_called_once_with(
            flight.id, dict(Delivered_Ad_Ops__c=1, Total_Flight_Cost__c=0))
