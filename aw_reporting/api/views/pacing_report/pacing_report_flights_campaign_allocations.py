from django.utils import timezone
from rest_framework.generics import UpdateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.models import Campaign
from aw_reporting.models import Flight
from aw_reporting.reports.pacing_report import PacingReport


class PacingReportFlightsCampaignAllocationsView(UpdateAPIView,
                                                 PacingReportHelper):
    queryset = Flight.objects.all()
    MIN_ALLOCATION_SUM = 99
    MAX_ALLOCATION_SUM = 101
    MIN_BUDGET = 0.01

    def get(self, *a, **kwargs):
        """
        Get all Campaigns associated with Flight

        :return: (list) Campaign objects
        """
        flight = self.get_object()
        data = PacingReport().get_campaigns(flight)
        self.multiply_percents(data)
        return Response(data=data)

    def update(self, request, *args, **kwargs):
        """
        Update Campaign budget allocations associated with flight
            Also updates both Campaign and associated Account update_times to mark for syncing with Adwords

        :return: (list) All flights
        """
        flight = self.get_object()
        report = PacingReport()
        # Ad operations require that for pacing reports, only running campaigns should be used
        campaign_ids = report.get_campaigns(flight, status="serving").values_list("id", flat=True)
        expected_keys = set(campaign_ids)
        try:
            flight_updated_budget = request.data.pop("flight_budget")
        except KeyError:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="You must provide a flight budget as \"flight_budget\""
            )
        if {int(key) for key in request.data.keys()} != expected_keys:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Wrong keys, expected: {}".format(expected_keys)
            )
        try:
            allocations = {
                int(_id): float(value) for _id, value in request.data.items()
            }
        except ValueError:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Invalid numerical values: {}".format(request.data.values())
            )
        if any((allocation / 100) * flight_updated_budget < self.MIN_BUDGET for allocation in allocations.values()):
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="All budget allocations must be greater than ${}.".format(self.MIN_BUDGET)
            )
        allocation_sum = sum(allocations.values())
        if not self.MIN_ALLOCATION_SUM <= round(allocation_sum) <= self.MAX_ALLOCATION_SUM:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Sum of the values is wrong: {}".format(allocation_sum)
            )

        Flight.objects.filter(
            id=flight.id
        ).update(
            budget=flight_updated_budget
        )

        for campaign_id, allocation_value in allocations.items():
            campaign_budget = (flight_updated_budget * allocation_value) / 100
            Campaign.objects.filter(pk=int(campaign_id)).update(
                goal_allocation=allocation_value,
                budget=campaign_budget,
                update_time=timezone.now()
            )
        res = self.get(request, *args, **kwargs)
        res.status_code = HTTP_202_ACCEPTED
        return res
