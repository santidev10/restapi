from rest_framework.generics import UpdateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_202_ACCEPTED

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.models import Flight, Campaign
from aw_reporting.reports.pacing_report import PacingReport


class PacingReportFlightsCampaignAllocationsView(UpdateAPIView,
                                                 PacingReportHelper):
    queryset = Flight.objects.all()

    def get(self, *a, **_):
        """
        Get all Campaigns associated with Flight
        :return: list -> Campaigns
        """
        flight = self.get_object()
        data = PacingReport().get_campaigns(flight)
        self.multiply_percents(data)
        return Response(data=data)

    def update(self, request, *args, **kwargs):
        """
        Update Campaign budget allocations associated with flight
        Update Campiagn.account.update_time to mark for updating from google ads scripts
        :return: list -> Flights
        """

        updated_at = request.data.get("updated_at")
        instance = self.get_object()
        # validation
        campaign_ids = Campaign.objects.filter(
            salesforce_placement=instance.placement
        ).values_list('id', flat=True)
        expected_keys = set(campaign_ids)
        if set(request.data.keys()) != expected_keys:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Wrong keys, expected: {}".format(expected_keys)
            )
        actual_sum = sum(request.data.values())
        if round(actual_sum) != 100:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="Sum of the values is wrong: {}".format(actual_sum)
            )
        # apply changes to CampaignCreation
        for campaign_id, allocation_value in request.data.items():
            Campaign.objects.filter(pk=campaign_id).update(
                goal_allocation=allocation_value,
                update_time=updated_at,
                account_update_time=updated_at,
            )

        # return
        res = self.get(request, *args, **kwargs)
        res.status_code = HTTP_202_ACCEPTED
        return res
