from rest_framework.generics import ListAPIView

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.csv_reports import PacingReportCSVExport


class PacingReportExportView(ListAPIView, PacingReportHelper):
    permission_classes = tuple()

    def get(self, request, *args, **kwargs):

        csv_generator = PacingReportCSVExport(request.GET)
        return csv_generator.prepare_csv_file_response()
