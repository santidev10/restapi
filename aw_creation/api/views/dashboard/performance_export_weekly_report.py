from datetime import datetime

from django.conf import settings
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.excel_reports import DashboardPerformanceWeeklyReport
from userprofile.constants import UserSettingsKey
from utils.views import xlsx_response


class DashboardPerformanceExportWeeklyReportApiView(APIView):
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
        queryset = AccountCreation.objects.all()
        user_settings = request.user.get_aw_settings()
        if not user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS):
            visible_accounts = user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
            queryset = queryset.filter(account_id__in=visible_accounts)
        try:
            item = queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        show_conversions = request.user.get_aw_settings().get(UserSettingsKey.SHOW_CONVERSIONS)
        report = DashboardPerformanceWeeklyReport(item.account, show_conversions, **filters)
        hide_brand_name = settings.CUSTOM_AUTH_FLAGS \
            .get(request.user.email.lower(), {}) \
            .get("hide_brand_name", False)
        report.hide_logo = hide_brand_name
        brand_name = "" if hide_brand_name else "Channel Factory"
        title = " ".join([f for f in [
            brand_name,
            item.name,
            "Weekly Report",
            datetime.now().date().strftime("%m.%d.%y")
        ] if f])
        return xlsx_response(title, report.get_content())