from rest_framework.generics import UpdateAPIView
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.response import Response

from aw_reporting.api.serializers.pacing_report_opportunity_update_serializer import \
    PacingReportOpportunityUpdateSerializer
from aw_reporting.models import Opportunity
from aw_reporting.reports.pacing_report import PacingReport
from .pacing_report_helper import PacingReportHelper


class PacingReportOpportunityBufferUpdateApiView(UpdateAPIView, PacingReportHelper):
    serializer_class = PacingReportOpportunityUpdateSerializer

    def update(self, request, pk, **_):
        """
        Update opportunity cpm_buffer or cpv_buffer values
        :param request: Request object
        :param pk: Opportunity id
        :return: Pacing Report data for Opportunity placements
        """
        try:
            opportunity = Opportunity.objects.get(pk=pk)
        except Opportunity.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND, data="Opportunity not found.")

        allowed_updates = ("cpm_buffer", "cpv_buffer")

        if not set(request.data.keys()).issubset(allowed_updates):
            return Response(status=HTTP_400_BAD_REQUEST, data="You may only update cpm_buffer or cpv_buffer values.")

        Opportunity.objects.filter(pk=pk).update(**request.data)
        query = {"search": opportunity.name}
        report = PacingReport()
        try:
            opportunity = report.get_opportunities(query, self.request.user)[0]
            data = {
                'plan_impressions': opportunity['plan_impressions'],
                'plan_video_views': opportunity['plan_video_views'],
            }
            status = HTTP_200_OK
        except IndexError:
            data = "Opportunity {} not found.".format(opportunity.name)
            status = HTTP_404_NOT_FOUND

        return Response(status=status, data=data)
