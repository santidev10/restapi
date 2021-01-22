from datetime import datetime
from collections import namedtuple

from django.db import transaction
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError

from aw_reporting.models import Flight
from aw_reporting.models import FlightPacingAllocation
from aw_reporting.utils import get_dates_range
from userprofile.constants import StaticPermissions
from utils.views import get_object
from utils.datetime import now_in_default_tz


Range = namedtuple("Range", ["start", "end"])


class PacingReportFlightAllocationAPIView(APIView):
    MIN_ALLOCATION_SUM = 99
    MAX_ALLOCATION_SUM = 101

    permission_classes = (StaticPermissions()(StaticPermissions.PACING_REPORT),)

    def patch(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        flight = get_object(Flight, id=pk)
        data = request.data
        data.sort(key=lambda allocation: allocation["start"])
        self._validate(flight, data)
        return Response()

    def _validate(self, flight, data):
        today = now_in_default_tz().date()

        allocations = FlightPacingAllocation.get_allocations(flight.id)
        total_allocation = sum(float(item["allocation"]) for item in data)
        if total_allocation != 100:
            raise ValidationError(
                f"Total allocations must be between: {self.MIN_ALLOCATION_SUM} - {self.MAX_ALLOCATION_SUM}.")

        with transaction.atomic():
            all_dates_ordinal = []
            for i, updated_allocation_range in enumerate(data):
                to_update = []
                start_date = self._parse_date(updated_allocation_range["start"])
                end_date = self._parse_date(updated_allocation_range["end"])

                if start_date > end_date:
                    raise ValidationError(f"Start date must be less than end date: {start_date} - {end_date}")

                # If modifying allocations for the first time, the first date range must include today's date
                if all(item.allocation == 100 for item in allocations.values()) and i == 0 and end_date < today:
                    raise ValidationError("You are trying to allocate pacing for the first time. "
                                          "Your first date range must include today's date.")

                for date in get_dates_range(start_date, end_date):
                    try:
                        allocation_obj = allocations[date]
                    except KeyError:
                        raise ValidationError(f"Date not in flight duration: {self._format_date(date)}")

                    updated_allocation_range_value = float(updated_allocation_range["allocation"])

                    # Validate if trying to change past allocation
                    if updated_allocation_range_value != allocation_obj.allocation and end_date < today:
                        raise ValidationError("You can not modify an allocation in a past date range.")

                    allocation_obj.allocation = updated_allocation_range_value

                    # Keep track which dates are the end of ranges
                    if date == end_date:
                        allocation_obj.is_end = True
                    else:
                        allocation_obj.is_end = False

                    to_update.append(allocation_obj)
                    all_dates_ordinal.append(date.toordinal())
                try:
                    # Check if overlapping date between date ranges
                    overlap = self._get_overlap(
                        start_date,
                        end_date,
                        self._parse_date(data[i + 1]["start"]),
                        self._parse_date(data[i + 1]["end"])
                    )
                    overlap = max(0, overlap)
                    if overlap > 0:
                        raise ValidationError("Dates must not overlap.")
                except IndexError:
                    # On last date range
                    pass

                # Validate that all date ranges are consecutive
                if max(all_dates_ordinal) - min(all_dates_ordinal) != len(all_dates_ordinal) - 1:
                    raise ValidationError("Date ranges must be consecutive.")

                FlightPacingAllocation.objects.bulk_update(to_update, fields=["allocation", "is_end"])

            # check first and last dates
            first = self._parse_date(data[0]["start"])
            last = self._parse_date(data[-1]["end"])
            if flight.start != first or flight.end != last:
                raise ValidationError(
                    "First date must start on flight start and last date must end on flight end date.")

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
