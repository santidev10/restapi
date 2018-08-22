from datetime import datetime

from django.db.models import Q
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.models import Account
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.excel_reports import PerformanceWeeklyReport
from utils.views import xlsx_response


@demo_view_decorator
class AnalyticsPerformanceExportWeeklyReportApiView(APIView):
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
        user = self.request.user
        related_accounts_ids = Account.user_objects(user).values_list("id", flat=True)
        queryset = AccountCreation.objects.filter(
            Q(is_deleted=False)
            & (Q(owner=user) | Q(account_id__in=related_accounts_ids))
        )
        try:
            item = queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        report = PerformanceWeeklyReport(item.account, **filters)

        title = "Channel Factory {} Weekly Report {}".format(
            item.name,
            datetime.now().date().strftime("%m.%d.%y")
        )
        return xlsx_response(title, report.get_content())
