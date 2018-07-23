from datetime import datetime

from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.excel_reports import PerformanceWeeklyReport
from aw_reporting.models import Account
from utils.views import xlsx_response


@demo_view_decorator
class AnalyzeExportWeeklyReportApiView(APIView):
    """
    Send filters to download weekly report

    Body example:

    {"campaigns": ["1", "2"]}
    """

    def get_filters(self):
        data = self.request.data
        filters = dict(
            campaigns=data.get("campaigns"),
            ad_groups=data.get("ad_groups"),
        )
        return filters

    def post(self, request, pk, **_):
        try:
            item = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        report = PerformanceWeeklyReport(item, **filters)

        title = "Channel Factory {} Weekly Report {}".format(
            item.name,
            datetime.now().date().strftime("%m.%d.%y")
        )
        return xlsx_response(title, report.get_content())
