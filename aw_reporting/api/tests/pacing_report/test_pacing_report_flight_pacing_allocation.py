from datetime import timedelta
import json

from django.utils import timezone
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import FlightPacingAllocation
from utils.unittests.test_case import ExtendedAPITestCase


class PacingReportFlightAllocationTestCase(ExtendedAPITestCase):
    def _get_url(self, flight_id):
        return reverse(f"aw_reporting_urls:{Name.PacingReport.FLIGHT_PACING_ALLOCATIONS}", kwargs=dict(pk=flight_id))

    def test_invalid_start_date(self):
        """ Test that start date of first date range must be start date of flight """
        self.create_admin_user()
        today = timezone.now()
        flight_start = today - timedelta(days=3)
        flight_end = today + timedelta(days=2)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3),
        )
        placement = OpPlacement.objects.create(
            id="2", name="pl", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            start=flight_start, end=today + timedelta(days=2),
        )
        flight = Flight.objects.create(
            id="3", placement=placement, start=flight_start, end=flight_end
        )
        border = flight.start + timedelta(days=1)
        payload = [
            dict(
                start=str((flight.start - timedelta(days=1)).date()),
                end=str(border.date()),
                allocation=50
            ),
            dict(
                start=str((border + timedelta(days=1)).date()),
                end=str(flight_end.date()),
                allocation=50
            )
        ]
        response = self.client.patch(self._get_url(flight.id), data=json.dumps(payload),
                                     content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_invalid_end_date(self):
        """ Test that end date of last date range must be end date of flight """
        self.create_admin_user()
        today = timezone.now()
        flight_start = today - timedelta(days=3)
        flight_end = today + timedelta(days=2)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3),
        )
        placement = OpPlacement.objects.create(
            id="2", name="Where is my money", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            start=flight_start, end=today + timedelta(days=2),
        )
        flight = Flight.objects.create(
            id="3", placement=placement, total_cost=200,
            start=flight_start, end=flight_end, ordered_units=10,
        )
        border = today
        payload = [
            dict(
                start=str(flight.start.date()),
                end=str(border.date()),
                allocation=50
            ),
            dict(
                start=str((border + timedelta(days=1)).date()),
                end=str((flight_end - timedelta(days=1)).date()),
                allocation=50
            )
        ]
        response = self.client.patch(self._get_url(flight.id), data=json.dumps(payload),
                                     content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertTrue("end" in str(response.data))

    def test_total_allocation_must_equal_100(self):
        """ Test that sum of allocations must be 100 """
        self.create_admin_user()
        today = timezone.now()
        flight_start = today - timedelta(days=3)
        flight_end = today + timedelta(days=2)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3),
        )
        placement = OpPlacement.objects.create(
            id="2", name="Where is my money", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            start=flight_start, end=today + timedelta(days=2),
        )
        flight = Flight.objects.create(
            id="3", placement=placement, total_cost=200,
            start=flight_start, end=flight_end, ordered_units=10,
        )
        border = flight.start + timedelta(days=1)
        payload = [
            dict(
                start=str(flight.start.date()),
                end=str(border.date()),
                allocation=90
            ),
            dict(
                start=str((border + timedelta(days=1)).date()),
                end=str(flight_end.date()),
                allocation=20
            )
        ]
        response = self.client.patch(self._get_url(flight.id), data=json.dumps(payload),
                                     content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertTrue("between" in str(response.data))

    def test_cannot_modify_past_allocation(self):
        """ Test that modifying an allocation in a date range that is in the past is not allowed """
        self.create_admin_user()
        today = timezone.now()
        flight_start = today - timedelta(days=3)
        border = flight_start
        flight_end = flight_start + timedelta(days=5)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3),
        )
        placement = OpPlacement.objects.create(
            id="2", name="pl", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            start=flight_start, end=today + timedelta(days=2),
        )
        flight = Flight.objects.create(
            id="3", placement=placement, total_cost=200,
            start=flight_start, end=flight_end, ordered_units=10,
        )
        allocations = FlightPacingAllocation.get_allocations(flight.id)
        # Just simulate allocations have been changed before
        FlightPacingAllocation.objects.filter(id=list(allocations.values())[0].id).update(allocation=10)
        payload = [
            dict(
                # Disallow changing allocation for date range in past
                start=str(flight.start.date()),
                end=str(border.date()),
                allocation=80
            ),
            dict(
                start=str((border + timedelta(days=1)).date()),
                end=str(flight_end.date()),
                allocation=20
            )
        ]
        response = self.client.patch(self._get_url(flight.id), data=json.dumps(payload),
                                     content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertTrue("past" in str(response.data))

    def test_invalid_overlapping_dates(self):
        """ Test that dates must not overlap between date ranges """
        self.create_admin_user()
        today = timezone.now()
        flight_start = today - timedelta(days=3)
        border = today
        flight_end = today + timedelta(days=2)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3),
        )
        placement = OpPlacement.objects.create(
            id="2", name="pl", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            start=flight_start, end=today + timedelta(days=2),
        )
        flight = Flight.objects.create(
            id="3", placement=placement, total_cost=200,
            start=flight_start, end=flight_end, ordered_units=10,
        )
        FlightPacingAllocation.get_allocations(flight.id)
        payload = [
            dict(
                start=str(flight.start.date()),
                end=str(border.date()),
                allocation=80
            ),
            dict(
                start=str((border - timedelta(days=1)).date()),
                end=str(flight_end.date()),
                allocation=20
            )
        ]
        response = self.client.patch(self._get_url(flight.id), data=json.dumps(payload),
                                     content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        print(response.data)
        self.assertTrue("overlap" in str(response.data))

    def test_first_allocation_modification_includes_today_date(self):
        """ Test that modifying allocations for the first time must include today's date """
        self.create_admin_user()
        today = timezone.now()
        flight_start = today - timedelta(days=3)
        border = today - timedelta(days=1)
        flight_end = border + timedelta(days=2)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3),
        )
        placement = OpPlacement.objects.create(
            id="2", name="pl", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            start=flight_start, end=today + timedelta(days=2),
        )
        flight = Flight.objects.create(
            id="3", placement=placement, total_cost=200,
            start=flight_start, end=flight_end, ordered_units=10,
        )
        FlightPacingAllocation.get_allocations(flight.id)
        payload = [
            dict(
                start=str(flight.start.date()),
                end=str(border.date()),
                allocation=80
            ),
            dict(
                start=str((border + timedelta(days=1)).date()),
                end=str(flight_end.date()),
                allocation=20
            )
        ]
        response = self.client.patch(self._get_url(flight.id), data=json.dumps(payload),
                                     content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertTrue("must include" in str(response.data))

    def test_success(self):
        self.create_admin_user()
        today = timezone.now()
        flight_start = today + timedelta(days=3)
        border = flight_start
        flight_end = border + timedelta(days=2)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3),
        )
        placement = OpPlacement.objects.create(
            id="2", name="pl", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            start=flight_start, end=today + timedelta(days=2),
        )
        flight = Flight.objects.create(
            id="3", placement=placement, total_cost=200,
            start=flight_start, end=flight_end, ordered_units=10,
        )
        allocations = FlightPacingAllocation.get_allocations(flight.id)
        # Just simulate allocations have been changed before
        FlightPacingAllocation.objects.filter(id=list(allocations.values())[0].id).update(allocation=10)
        payload = [
            dict(
                start=str(flight.start.date()),
                end=str(border.date()),
                allocation=80
            ),
            dict(
                start=str((border + timedelta(days=1)).date()),
                end=str(flight_end.date()),
                allocation=20
            )
        ]
        response = self.client.patch(self._get_url(flight.id), data=json.dumps(payload),
                                     content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
