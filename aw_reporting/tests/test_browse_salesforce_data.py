from datetime import date
from datetime import timedelta
from unittest.mock import MagicMock
from unittest.mock import patch

from django.core import mail
from django.core.management import call_command as django_call_command
from django.test import TransactionTestCase
from django.test import override_settings

from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import User
from aw_reporting.models.salesforce_constants import DynamicPlacementType, SalesforceFields
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import get_pacing_from_flights
from aw_reporting.salesforce import Connection
from email_reports.reports.base import BaseEmailReport
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now


def call_command(*args, **kwargs):
    with patch("aw_reporting.management.commands.browse_salesforce_data.logger.exception") as exception_mock:
        django_call_command(*args, **kwargs)
        if exception_mock.called:
            raise exception_mock.call_args[0][0]


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
            opportunity_data(
                Id="123",
                Name="",
            )
        ])
        with patch(
                "aw_reporting.management.commands.browse_salesforce_data.SConnection",
                return_value=sf_mock):
            call_command("browse_salesforce_data", no_update="1")

        self.assertEqual(Opportunity.objects.all().count(), 1)

    def test_notify_ordered_units_changed(self):
        ordered_units = 123
        ad_ops = User.objects.create(
            id=str(next(int_iterator)),
            name="Paul",
            email="1@mail.cz"
        )
        opportunity = Opportunity.objects.create(
            id=str(next(int_iterator)),
            name="Some Opportunity #123",
            ad_ops_manager=ad_ops,
        )
        placement = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            ordered_units=ordered_units,
        )
        flight = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement,
            ordered_units=ordered_units,
        )

        new_ordered_units = ordered_units + 13

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("User", [
            dict(
                Id=ad_ops.id,
                Name=ad_ops.name,
                Email=ad_ops.email,
                IsActive=True,
                UserRoleId=None,
            )
        ])
        sf_mock.add_mocked_items("Opportunity", [
            opportunity_data(
                Id=opportunity.id,
                Name=opportunity.name,
                Ad_Ops_Campaign_Manager_UPDATE__c=ad_ops.id,
            )
        ])
        sf_mock.add_mocked_items("Placement__c", [
            placement_data(
                Id=placement.id,
                Name=placement.name,
                Insertion_Order__c=placement.opportunity_id,
            )
        ])
        sf_mock.add_mocked_items("Flight__c", [
            flight_data(
                Id=flight.id,
                Name=flight.name,
                Ordered_Units__c=new_ordered_units,
                Placement__c=flight.placement_id,
            )
        ])

        with patch("aw_reporting.management.commands.browse_salesforce_data.SConnection", return_value=sf_mock), \
             override_settings(DEBUG_EMAIL_NOTIFICATIONS=False):
            call_command("browse_salesforce_data", no_update="1")

        flight.refresh_from_db()
        self.assertEqual(flight.ordered_units, new_ordered_units)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        self.assertEqual(mail.outbox[0].to[0], ad_ops.email)
        self.assertEqual(email.subject, "{} Ordered Units has changed".format(opportunity.name))
        expected_body_lines = [
            "Flight: {}".format(flight.name),
            "Placement: {}".format(placement.name),
            "Change: The ordered units were changed from {old_value} to {new_value}".format(
                old_value=ordered_units,
                new_value=new_ordered_units,
            ),
        ]
        expected_body = "\n\n".join(expected_body_lines)
        self.assertEqual(email.body, expected_body)

    def test_dynamic_placement_notify_total_cost_changed(self):
        total_cost = 123.
        ad_ops = User.objects.create(
            id=str(next(int_iterator)),
            name="Paul",
            email="1@mail.cz"
        )
        opportunity = Opportunity.objects.create(
            id=str(next(int_iterator)),
            name="Some Opportunity #123",
            ad_ops_manager=ad_ops,
        )
        placement = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=total_cost,
        )
        flight = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement,
            total_cost=total_cost,
        )

        new_total_cost = total_cost + 13

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("User", [
            dict(
                Id=ad_ops.id,
                Name=ad_ops.name,
                Email=ad_ops.email,
                IsActive=True,
                UserRoleId=None,
            )
        ])
        sf_mock.add_mocked_items("Opportunity", [
            opportunity_data(
                Id=opportunity.id,
                Name=opportunity.name,
                Ad_Ops_Campaign_Manager_UPDATE__c=ad_ops.id,
            )
        ])
        sf_mock.add_mocked_items("Placement__c", [
            placement_data(
                Id=placement.id,
                Name=placement.name,
                Insertion_Order__c=placement.opportunity_id,
                Dynamic_Placement__c=placement.dynamic_placement,
            )
        ])
        sf_mock.add_mocked_items("Flight__c", [
            flight_data(
                Id=flight.id,
                Name=flight.name,
                Flight_Value__c=new_total_cost,
                Placement__c=flight.placement_id,
            )
        ])

        with patch("aw_reporting.management.commands.browse_salesforce_data.SConnection", return_value=sf_mock), \
             override_settings(DEBUG_EMAIL_NOTIFICATIONS=False):
            call_command("browse_salesforce_data", no_update="1")

        flight.refresh_from_db()
        self.assertEqual(flight.total_cost, new_total_cost)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        self.assertEqual(mail.outbox[0].to[0], ad_ops.email)
        self.assertEqual(email.subject, "{} Total Client Cost has changed".format(opportunity.name))
        expected_body_lines = [
            "Flight: {}".format(flight.name),
            "Placement: {}".format(placement.name),
            "Change: The total client cost was changed from {old_value} to {new_value}".format(
                old_value=total_cost,
                new_value=new_total_cost,
            ),
        ]
        expected_body = "\n\n".join(expected_body_lines)
        self.assertEqual(email.body, expected_body)

    def test_update_no_notification_if_not_changed(self):
        ad_ops = User.objects.create(
            id=str(next(int_iterator)),
            name="Paul",
            email="1@mail.cz"
        )
        opportunity = Opportunity.objects.create(
            id=str(next(int_iterator)),
            name="Some Opportunity #123",
            ad_ops_manager=ad_ops,
        )
        placement_dynamic = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=100,
            ordered_units=100,
        )
        flight_dynamic = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement_dynamic,
            total_cost=100,
            ordered_units=100,
        )

        placement_regular = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            total_cost=100,
            ordered_units=100,
        )
        flight_regular = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement_regular,
            total_cost=100,
            ordered_units=100,
        )

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("User", [
            dict(
                Id=ad_ops.id,
                Name=ad_ops.name,
                Email=ad_ops.email,
                IsActive=True,
                UserRoleId=None,
            )
        ])
        sf_mock.add_mocked_items("Opportunity", [
            opportunity_data(
                Id=opportunity.id,
                Name=opportunity.name,
                Ad_Ops_Campaign_Manager_UPDATE__c=ad_ops.id,
            )
        ])
        sf_mock.add_mocked_items("Placement__c", [
            placement_data(
                Id=placement.id,
                Name=placement.name,
                Insertion_Order__c=placement.opportunity_id,
                Dynamic_Placement__c=placement.dynamic_placement,
            )
            for placement in [placement_dynamic, placement_regular]
        ])
        sf_mock.add_mocked_items("Flight__c", [
            flight_data(
                Id=flight_dynamic.id,
                Name=flight_dynamic.name,
                Flight_Value__c=flight_dynamic.total_cost,
                Ordered_Units__c=flight_dynamic.ordered_units + 1,
                Placement__c=flight_dynamic.placement_id,
            ),
            flight_data(
                Id=flight_regular.id,
                Name=flight_regular.name,
                Flight_Value__c=flight_regular.total_cost + 1,
                Ordered_Units__c=flight_regular.ordered_units,
                Placement__c=flight_regular.placement_id,
            ),
        ])

        with patch("aw_reporting.management.commands.browse_salesforce_data.SConnection", return_value=sf_mock):
            call_command("browse_salesforce_data", no_update="1")

        self.assertEqual(len(mail.outbox), 0)

    def test_notify_ordered_units_changed_no_ad_ops(self):
        ordered_units = 123
        opportunity = Opportunity.objects.create(
            id=str(next(int_iterator)),
            name="Some Opportunity #123",
            ad_ops_manager=None,
        )
        placement = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            ordered_units=ordered_units,
        )
        flight = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement,
            ordered_units=ordered_units,
        )

        new_ordered_units = ordered_units + 13

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("Opportunity", [
            opportunity_data(
                Id=opportunity.id,
                Name=opportunity.name,
            )
        ])
        sf_mock.add_mocked_items("Placement__c", [
            placement_data(
                Id=placement.id,
                Name=placement.name,
                Insertion_Order__c=placement.opportunity_id,
            )
        ])
        sf_mock.add_mocked_items("Flight__c", [
            flight_data(
                Id=flight.id,
                Name=flight.name,
                Ordered_Units__c=new_ordered_units,
                Placement__c=flight.placement_id,
            )
        ])
        test_email = "test@mail.com"
        with patch("aw_reporting.management.commands.browse_salesforce_data.SConnection", return_value=sf_mock), \
             override_settings(SALESFORCE_UPDATES_ADDRESSES=[test_email]), \
             override_settings(DEBUG_EMAIL_NOTIFICATIONS=False):
            call_command("browse_salesforce_data", no_update="1")

        flight.refresh_from_db()
        self.assertEqual(flight.ordered_units, new_ordered_units)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        self.assertEqual(mail.outbox[0].to[0], test_email)
        self.assertEqual(email.subject, "{} Ordered Units has changed".format(opportunity.name))

    def test_dynamic_placement_notify_total_cost_changed_no_ad_ops(self):
        total_cost = 123.
        opportunity = Opportunity.objects.create(
            id=str(next(int_iterator)),
            name="Some Opportunity #123",
        )
        placement = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=total_cost,
        )
        flight = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement,
            total_cost=total_cost,
        )

        new_total_cost = total_cost + 13

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("Opportunity", [
            opportunity_data(
                Id=opportunity.id,
                Name=opportunity.name,
            )
        ])
        sf_mock.add_mocked_items("Placement__c", [
            placement_data(
                Id=placement.id,
                Name=placement.name,
                Insertion_Order__c=placement.opportunity_id,
                Dynamic_Placement__c=placement.dynamic_placement,
            )
        ])
        sf_mock.add_mocked_items("Flight__c", [
            flight_data(
                Id=flight.id,
                Name=flight.name,
                Flight_Value__c=new_total_cost,
                Placement__c=flight.placement_id,
            )
        ])

        test_email = "test@mail.com"
        with patch("aw_reporting.management.commands.browse_salesforce_data.SConnection", return_value=sf_mock), \
             override_settings(SALESFORCE_UPDATES_ADDRESSES=[test_email]), \
             override_settings(DEBUG_EMAIL_NOTIFICATIONS=False):
            call_command("browse_salesforce_data", no_update="1")

        flight.refresh_from_db()
        self.assertEqual(flight.total_cost, new_total_cost)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        self.assertEqual(mail.outbox[0].to[0], test_email)
        self.assertEqual(email.subject, "{} Total Client Cost has changed".format(opportunity.name))

    def test_no_notifications_if_probability_low(self):
        ad_ops = User.objects.create(
            id=str(next(int_iterator)),
            name="Paul",
            email="1@mail.cz"
        )
        opportunity = Opportunity.objects.create(
            id=str(next(int_iterator)),
            name="Some Opportunity #123",
            ad_ops_manager=ad_ops,
        )
        placement_dynamic = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=100,
            ordered_units=100,
        )
        flight_dynamic = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement_dynamic,
            total_cost=100,
            ordered_units=100,
        )

        placement_regular = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            total_cost=100,
            ordered_units=100,
        )
        flight_regular = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement_regular,
            total_cost=100,
            ordered_units=100,
        )

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("User", [
            dict(
                Id=ad_ops.id,
                Name=ad_ops.name,
                Email=ad_ops.email,
                IsActive=True,
                UserRoleId=None,
            )
        ])
        sf_mock.add_mocked_items("Opportunity", [
            opportunity_data(
                Id=opportunity.id,
                Name=opportunity.name,
                Ad_Ops_Campaign_Manager_UPDATE__c=ad_ops.id,
                Probability=99,
            )
        ])
        sf_mock.add_mocked_items("Placement__c", [
            placement_data(
                Id=placement.id,
                Name=placement.name,
                Insertion_Order__c=placement.opportunity_id,
                Dynamic_Placement__c=placement.dynamic_placement,
            )
            for placement in [placement_dynamic, placement_regular]
        ])
        sf_mock.add_mocked_items("Flight__c", [
            flight_data(
                Id=flight_dynamic.id,
                Name=flight_dynamic.name,
                Flight_Value__c=flight_dynamic.total_cost + 1,
                Ordered_Units__c=flight_dynamic.ordered_units + 1,
                Placement__c=flight_dynamic.placement_id,
            ),
            flight_data(
                Id=flight_regular.id,
                Name=flight_regular.name,
                Flight_Value__c=flight_regular.total_cost + 1,
                Ordered_Units__c=flight_regular.ordered_units + 1,
                Placement__c=flight_regular.placement_id,
            ),
        ])

        with patch("aw_reporting.management.commands.browse_salesforce_data.SConnection", return_value=sf_mock):
            call_command("browse_salesforce_data", no_update="1")

        self.assertEqual(len(mail.outbox), 0)

    def test_notify_ordered_units_changed_debug(self):
        ordered_units = 123
        ad_ops = User.objects.create(
            id=str(next(int_iterator)),
            name="Paul",
            email="1@mail.cz"
        )
        opportunity = Opportunity.objects.create(
            id=str(next(int_iterator)),
            name="Some Opportunity #123",
            ad_ops_manager=ad_ops,
        )
        placement = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            ordered_units=ordered_units,
        )
        flight = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement,
            ordered_units=ordered_units,
        )

        new_ordered_units = ordered_units + 13

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("User", [
            dict(
                Id=ad_ops.id,
                Name=ad_ops.name,
                Email=ad_ops.email,
                IsActive=True,
                UserRoleId=None,
            )
        ])
        sf_mock.add_mocked_items("Opportunity", [
            opportunity_data(
                Id=opportunity.id,
                Name=opportunity.name,
                Ad_Ops_Campaign_Manager_UPDATE__c=ad_ops.id,
            )
        ])
        sf_mock.add_mocked_items("Placement__c", [
            placement_data(
                Id=placement.id,
                Name=placement.name,
                Insertion_Order__c=placement.opportunity_id,
            )
        ])
        sf_mock.add_mocked_items("Flight__c", [
            flight_data(
                Id=flight.id,
                Name=flight.name,
                Ordered_Units__c=new_ordered_units,
                Placement__c=flight.placement_id,
            )
        ])

        with patch("aw_reporting.management.commands.browse_salesforce_data.SConnection", return_value=sf_mock), \
             override_settings(DEBUG_EMAIL_NOTIFICATIONS=True):
            call_command("browse_salesforce_data", no_update="1")

        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].to[0], BaseEmailReport.DEBUG_PREFIX + ad_ops.email)

    def test_dynamic_placement_notify_total_cost_changed_debug(self):
        total_cost = 123.
        ad_ops = User.objects.create(
            id=str(next(int_iterator)),
            name="Paul",
            email="1@mail.cz"
        )
        opportunity = Opportunity.objects.create(
            id=str(next(int_iterator)),
            name="Some Opportunity #123",
            ad_ops_manager=ad_ops,
        )
        placement = OpPlacement.objects.create(
            id=str(next(int_iterator)),
            name="Some placement #234",
            opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=total_cost,
        )
        flight = Flight.objects.create(
            id=str(next(int_iterator)),
            name="Some flight #345",
            placement=placement,
            total_cost=total_cost,
        )

        new_total_cost = total_cost + 13

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("User", [
            dict(
                Id=ad_ops.id,
                Name=ad_ops.name,
                Email=ad_ops.email,
                IsActive=True,
                UserRoleId=None,
            )
        ])
        sf_mock.add_mocked_items("Opportunity", [
            opportunity_data(
                Id=opportunity.id,
                Name=opportunity.name,
                Ad_Ops_Campaign_Manager_UPDATE__c=ad_ops.id,
            )
        ])
        sf_mock.add_mocked_items("Placement__c", [
            placement_data(
                Id=placement.id,
                Name=placement.name,
                Insertion_Order__c=placement.opportunity_id,
                Dynamic_Placement__c=placement.dynamic_placement,
            )
        ])
        sf_mock.add_mocked_items("Flight__c", [
            flight_data(
                Id=flight.id,
                Name=flight.name,
                Flight_Value__c=new_total_cost,
                Placement__c=flight.placement_id,
            )
        ])

        with patch("aw_reporting.management.commands.browse_salesforce_data.SConnection", return_value=sf_mock), \
             override_settings(DEBUG_EMAIL_NOTIFICATIONS=True):
            call_command("browse_salesforce_data", no_update="1")

        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].to[0], BaseEmailReport.DEBUG_PREFIX + ad_ops.email)

    def test_get_flight_pacing(self):
        self.assertEqual(Opportunity.objects.all().count(), 0)
        opportunity_id = next(int_iterator)
        placement_id = next(int_iterator)
        flight_id = next(int_iterator)
        pacing = 0.234

        sf_mock = MockSalesforceConnection()
        sf_mock.add_mocked_items("Opportunity", [
            opportunity_data(
                Id=opportunity_id,
                Name="",
            )
        ])
        sf_mock.add_mocked_items("Placement__c", [
            placement_data(
                Id=placement_id,
                Name="",
                Insertion_Order__c=opportunity_id,
            )
        ])
        sf_mock.add_mocked_items("Flight__c", [
            flight_data(
                Id=flight_id,
                Name="",
                Pacing__c=pacing,
                Placement__c=placement_id,
            )
        ])
        with patch(
                "aw_reporting.management.commands.browse_salesforce_data.SConnection",
                return_value=sf_mock):
            call_command("browse_salesforce_data", no_update="1")

        self.assertEqual(Flight.objects.all().count(), 1)
        self.assertEqual(Flight.objects.all().first().pacing, pacing)

    def test_update_pacing_changed(self):
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        start = date(2017, 1, 1)
        end = today = start + timedelta(days=1)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
        )
        flight = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            start=start, end=end,
            delivered=999,
            ordered_units=123,
            cost=999,
        )

        campaign = Campaign.objects.create(
            salesforce_placement=placement
        )
        CampaignStatistic.objects.create(
            date=start,
            campaign=campaign,
            cost=999,
            impressions=999,
        )
        with patch_now(today):
            pacing_report = PacingReport()
            flights_data = pacing_report.get_flights_data(id=flight.id)
            pacing = get_pacing_from_flights(flights_data)
            self.assertNotEqual(pacing, flight.pacing)
        flight.refresh_from_db()

        sf_mock = MagicMock()
        sf_mock().sf.Flight__c.update.return_value = 204
        sf_mock().sf.Placement__c.update.return_value = 204

        with patch("aw_reporting.management.commands"
                   ".browse_salesforce_data.SConnection", new=sf_mock), \
             patch_now(today):
            call_command("browse_salesforce_data", no_get="1")

        sf_mock().sf.Flight__c.update.assert_called_once_with(
            flight.id, dict(Pacing__c=pacing))

    def test_update_pacing_changed(self):
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        start = date(2017, 1, 1)
        end = today = start + timedelta(days=1)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
        )
        flight = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            start=start, end=end,
            delivered=999,
            ordered_units=123,
            cost=999,
        )

        campaign = Campaign.objects.create(
            salesforce_placement=placement
        )
        CampaignStatistic.objects.create(
            date=start,
            campaign=campaign,
            cost=999,
            impressions=999,
        )
        with patch_now(today):
            pacing_report = PacingReport()
            flights_data = pacing_report.get_flights_data(id=flight.id)
            pacing = get_pacing_from_flights(flights_data)
            self.assertIsNotNone(pacing, flight.pacing)
        flight.pacing = pacing - 1e-08
        flight.save()
        flight.refresh_from_db()

        sf_mock = MagicMock()
        sf_mock().sf.Flight__c.update.return_value = 204
        sf_mock().sf.Placement__c.update.return_value = 204

        with patch("aw_reporting.management.commands"
                   ".browse_salesforce_data.SConnection", new=sf_mock), \
             patch_now(today):
            call_command("browse_salesforce_data", no_get="1")

        sf_mock().sf.Flight__c.update.assert_not_called()


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
            "fields": [{"name": "Client_Vertical__c", "picklistValues": []}]
        }


def opportunity_data(**kwargs):
    default_values = dict(
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
        Bill_off_3p_Numbers__c=False,
        CID_Google_Transparency_Required__c=False,
    )
    return {
        **default_values,
        **kwargs,
    }


def placement_data(**kwargs):
    default_values = dict(
        Name="",
        Cost_Method__c="CPM",
        Insertion_Order__c=None,
        Total_Ordered_Units__c=0,
        Ordered_Cost_Per_Unit__c=0,
        Total_Client_Costs__c=0,
        Placement_Start_Date__c=None,
        Placement_End_Date__c=None,
        PLACEMENT_ID_Number__c=None,
        Adwords_Placement_IQ__c=None,
        Incremental__c=False,
        Placement_Type__c=None,
        Dynamic_Placement__c=None,
        Tech_Fee_if_applicable__c=None,
        Tech_Fee_Cap_if_applicable__c=None,
        Tech_Fee_Type__c=None,
    )
    return {
        **default_values,
        **kwargs,
    }


def flight_data(**kwargs):
    default_values = dict(
        Name="",
        Placement__c=None,
        Flight_Start_Date__c=None,
        Flight_End_Date__c=None,
        Flight_Month__c=None,
        Total_Flight_Cost__c=0,
        Flight_Value__c=0,
        Delivered_Ad_Ops__c=None,
        Ordered_Amount__c=0,
        Ordered_Units__c=0,
        Pacing__c=None,
    )
    return {
        **default_values,
        **kwargs,
    }
