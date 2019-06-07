from rest_framework.generics import UpdateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_202_ACCEPTED

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.models import Campaign
from aw_reporting.models import Flight
from aw_reporting.reports.pacing_report import PacingReport
from django.utils import timezone


class PacingReportFlightsCampaignAllocationsView(UpdateAPIView,
                                                 PacingReportHelper):
    queryset = Flight.objects.all()
    MIN_ALLOCATION_SUM = 99
    MAX_ALLOCATION_SUM = 101

    def get(self, *a, **_):
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
        instance = self.get_object()
        # validation
        campaign_ids = Campaign.objects.filter(
            salesforce_placement=instance.placement
        ).values_list('id', flat=True)
        expected_keys = set(campaign_ids)

        try:
            flight_updated_budget = request.data.pop('flight_budget')
        except KeyError:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data='You must provide a flight budget as "flight_budget"'
            )
        if set(request.data.keys()) != expected_keys:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Wrong keys, expected: {}".format(expected_keys)
            )
        try:
            allocations = {
                _id: float(value) for _id, value in request.data.items()
            }
        except ValueError:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Invalid numeric values: {}".format(request.data.values())
            )

        allocation_sum = sum(allocations.values())
        if not self.MIN_ALLOCATION_SUM <= round(allocation_sum) <= self.MAX_ALLOCATION_SUM:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Sum of the values is wrong: {}".format(allocation_sum)
            )

        Flight.objects.filter(
            id=instance.id
        ).update(
            budget=flight_updated_budget
        )

        for campaign_id, allocation_value in allocations.items():
            campaign_budget = (flight_updated_budget * allocation_value) / 100
            Campaign.objects.filter(pk=campaign_id).update(
                goal_allocation=allocation_value,
                budget=campaign_budget,
                update_time=timezone.now()
            )
        res = self.get(request, *args, **kwargs)
        res.status_code = HTTP_202_ACCEPTED
        return res
