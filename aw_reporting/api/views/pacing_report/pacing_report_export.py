from rest_framework.generics import ListAPIView

from aw_reporting import xlsx_reports
from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.reports.pacing_report import PacingReport
from utils.views import xlsx_response


class PacingReportExportView(ListAPIView, PacingReportHelper):
    permission_classes = tuple()

    def get(self, request, *args, **kwargs):
        report = PacingReport()
        opportunities = report.get_opportunities(request.GET)

        report = xlsx_reports.pacing_report(report, opportunities)

        return xlsx_response(report.name, report)
