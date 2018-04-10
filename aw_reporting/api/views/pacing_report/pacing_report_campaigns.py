from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.models import Flight
from aw_reporting.reports.pacing_report import PacingReport


class PacingReportCampaignsApiView(ListAPIView, PacingReportHelper):

    def get(self, request, pk, **_):
        try:
            flight = Flight.objects.get(pk=pk)
        except Flight.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        report = PacingReport()
        campaigns = report.get_campaigns(flight)
        self.multiply_percents(campaigns)
        return Response(campaigns)
