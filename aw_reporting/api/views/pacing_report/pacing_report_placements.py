from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_reporting.models import Opportunity
from aw_reporting.reports.pacing_report import PacingReport
from .pacing_report_helper import PacingReportHelper


class PacingReportPlacementsApiView(ListAPIView, PacingReportHelper):
    def get(self, request, pk, **_):
        try:
            opportunity = Opportunity.objects.get(pk=pk)
        except Opportunity.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        report = PacingReport()
        placements = report.get_placements(opportunity)
        self.multiply_percents(placements)
        return Response(placements)
