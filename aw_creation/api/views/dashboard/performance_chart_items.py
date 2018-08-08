from datetime import datetime

from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.charts import DeliveryChart
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import DATE_FORMAT
from userprofile.models import UserSettingsKey
from utils.permissions import UserHasDashboardPermission


@demo_view_decorator
class DashboardPerformanceChartItemsApiView(APIView):
    """
    Send filters to get a list of targeted items

    Body example:

    {"segmented": false}
    """
    permission_classes = (IsAuthenticated, UserHasDashboardPermission)

    def get_filters(self):
        data = self.request.data
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        filters = dict(
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
            campaigns=data.get("campaigns"),
            ad_groups=data.get("ad_groups"),
            segmented_by=data.get("segmented"))
        return filters

    def post(self, request, pk, **kwargs):
        dimension = kwargs.get('dimension')
        queryset = AccountCreation.objects.all()
        user_settings = request.user.get_aw_settings()
        if not user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS):
            visible_accounts = user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
            queryset = queryset.filter(account__id__in=visible_accounts)
        try:
            item = queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        filters = self.get_filters()
        accounts = []
        if item.account:
            accounts.append(item.account.id)
        chart = DeliveryChart(
            accounts=accounts,
            dimension=dimension,
            show_conversions=user_settings.get(UserSettingsKey.SHOW_CONVERSIONS),
            **filters)
        data = chart.get_items()
        return Response(data=data)
