from datetime import datetime
from collections import namedtuple

from django.utils import timezone
from django.db import transaction
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError

from aw_reporting.models import Flight
from aw_reporting.models import FlightPacingGoal
from aw_reporting.utils import get_dates_range
from utils.views import get_object

Range = namedtuple("Range", ["start", "end"])


class PacingReportFlightAllocationAPIView(APIView):
    def patch(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        flight = get_object(Flight, id=pk)
        data = request.data
        data.sort(key=lambda allocation: allocation["start"])
        self._validate(flight, data)
        return Response()

    def _validate(self, flight, data):
        today = timezone.now().date()
        allocations = FlightPacingGoal.get_flight_pacing_goals(flight.id)
        total_allocation = sum(int(item["allocation"]) for item in data)
        if total_allocation != 100:
            raise ValidationError("Allocations must have a sum of 100.")

        # check first and last dates
        first = self._parse_date(data[0]["start"])
        last = self._parse_date(data[-1]["end"])
        if flight.start != first or flight.end != last:
            raise ValidationError("Date ranges must start on flight start and end on flight end dates.")

        with transaction.atomic():
            for i, updated_allocation in enumerate(data):
                to_update = []
                start_date = self._parse_date(updated_allocation["start"])
                end_date = self._parse_date(updated_allocation["end"])
                for date in get_dates_range(start_date, end_date):
                    allocation_obj = allocations[date]
                    updated_allocation_value = float(updated_allocation["allocation"])
                    # Reject modifying past allocations
                    if date < today and updated_allocation_value != allocation_obj.allocation:
                        raise ValidationError("You can not modify a past allocation.")
                    allocation_obj.allocation = updated_allocation_value
                    to_update.append(allocation_obj)
                try:
                    # Check if overlapping date
                    overlap = self._get_overlap(
                        self._parse_date(updated_allocation["start"]),
                        self._parse_date(updated_allocation["end"]),
                        self._parse_date(data[i + 1]["start"]),
                        self._parse_date(data[i + 1]["end"])
                    )
                    overlap = max(0, overlap)
                    if overlap > 0:
                        raise ValidationError("Dates must not overlap.")
                except IndexError:
                    # On last date range
                    pass
                FlightPacingGoal.objects.bulk_update(to_update, fields=["allocation"])

    def _get_overlap(self, s1, e1, s2, e2):
        """
        Check for overlapping dates
        """
        r1 = Range(start=s1, end=e1)
        r2 = Range(start=s2, end=e2)
        latest_start = max(r1.start, r2.start)
        earliest_end = min(r1.end, r2.end)
        delta = (earliest_end - latest_start).days + 1
        overlap = max(0, delta)
        return overlap

    def _parse_date(self, date_str):
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        return date

    def _format_date(self, date):
        date_str = date.strftime("%Y-%m-%d")
        return date_str