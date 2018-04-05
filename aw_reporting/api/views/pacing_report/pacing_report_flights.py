from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_reporting.models import OpPlacement
from aw_reporting.reports.pacing_report import PacingReport
from .pacing_report_helper import PacingReportHelper


class PacingReportFlightsApiView(ListAPIView, PacingReportHelper):
    def get(self, request, pk, **_):
        try:
            placement = OpPlacement.objects.get(pk=pk)
        except OpPlacement.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        report = PacingReport()
        flights = report.get_flights(placement)
        self.multiply_percents(flights)
        return Response(flights)
