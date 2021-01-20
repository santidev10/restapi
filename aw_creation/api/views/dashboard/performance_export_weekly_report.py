from datetime import datetime

from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.excel_reports import DashboardPerformanceWeeklyReport
from userprofile.constants import UserSettingsKey
from userprofile.permissions import PermissionGroupNames
from utils.views import xlsx_response
from userprofile.constants import StaticPermissions


class DashboardPerformanceExportWeeklyReportApiView(APIView):
    """
    Send filters to download weekly report

    Body example:

    {"campaigns": ["1", "2"]}
    """
    permission_classes = (
        StaticPermissions()(StaticPermissions.MANAGED_SERVICE__EXPORT),
    )

    def get_filters(self):
        data = self.request.data
        filters = dict(
            campaigns=data.get("campaigns"),
            ad_groups=data.get("ad_groups"),
        )
        return filters

    def post(self, request, pk, **_):
        user = request.user
        queryset = AccountCreation.objects.all()
        user_settings = user.get_aw_settings()
        if not user.has_permission(StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS):
            visible_accounts = user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
            queryset = queryset.filter(account_id__in=visible_accounts)
        try:
            item = queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        show_conversions = user.has_permission(StaticPermissions.MANAGED_SERVICE__CONVERSIONS)
        managed_service_hide_delivery_data = not user.has_permission(StaticPermissions.MANAGED_SERVICE__DELIVERY)
        report = DashboardPerformanceWeeklyReport(
            item.account, show_conversions, managed_service_hide_delivery_data,
            **filters
        )
        title = " ".join([f for f in [
            "ViewIQ",
            item.name,
            "Weekly Report",
            datetime.now().date().strftime("%m.%d.%y")
        ] if f])
        return xlsx_response(title, report.get_content())
