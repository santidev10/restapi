from rest_framework.generics import ListAPIView

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.csv_reports import PacingReportCSVExport
from aw_reporting.reports.pacing_report import PacingReport


class PacingReportExportView(ListAPIView, PacingReportHelper):
    permission_classes = tuple()

    def get(self, request, *args, **kwargs):
        pacing_report = PacingReport()
        opportunities = pacing_report.get_opportunities(request.GET)

        csv_generator = PacingReportCSVExport(pacing_report, opportunities, pacing_report.name)
        return csv_generator.prepare_csv_file_response()
