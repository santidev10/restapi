from datetime import datetime

from django.http import HttpResponse
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.excel_reports import PerformanceWeeklyReport


@demo_view_decorator
class PerformanceExportWeeklyReport(APIView):
    """
    Send filters to download weekly report

    Body example:

    {"campaigns": ["1", "2"]}
    """

    def get_filters(self):
        data = self.request.data
        filters = dict(
            campaigns=data.get('campaigns'),
            ad_groups=data.get('ad_groups'),
        )
        return filters

    def post(self, request, pk, **_):
        try:
            item = AccountCreation.objects.filter(owner=request.user).get(
                pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        report = PerformanceWeeklyReport(item.account, **filters)

        response = HttpResponse(
            report.get_content(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response[
            'Content-Disposition'] = 'attachment; filename="Channel Factory {} Weekly ' \
                                     'Report {}.xlsx"'.format(
            item.name,
            datetime.now().date().strftime("%m.%d.%y")
        )
        return response
