from django.http import HttpResponse
from rest_framework.generics import ListAPIView

from aw_reporting import xlsx_reports
from aw_reporting.api.views.pacing_report.pacing_report_helper import PacingReportHelper
from aw_reporting.reports.pacing_report import PacingReport


class PacingReportExportView(ListAPIView, PacingReportHelper):

    permission_classes = tuple()

    def get(self, request, *args, **kwargs):
        report = PacingReport()
        opportunities = report.get_opportunities(request.GET, self.request.user)
        response = HttpResponse(
            xlsx_reports.pacing_report(report, opportunities),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename="{}"'.format("{}.xlsx".format(report.name))
        return response