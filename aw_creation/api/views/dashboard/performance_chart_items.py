from datetime import datetime

from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.api.serializers.common.utils import get_currency_code
from aw_creation.models import AccountCreation
from aw_reporting.charts.dashboard_charts import DeliveryChart
from aw_reporting.models import DATE_FORMAT
from aw_reporting.models import MANAGED_SERVICE_DELIVERY_DATA
from userprofile.constants import StaticPermissions


class DashboardPerformanceChartItemsApiView(APIView):
    """
    Send filters to get a list of targeted items

    Body example:

    {"segmented": false}
    """
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MANAGED_SERVICE),)

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
        dimension = kwargs.get("dimension")
        queryset = AccountCreation.objects.all()
        if not request.user.has_permission(StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS):
            visible_accounts = request.user.get_visible_accounts_list()
            queryset = queryset.filter(account__id__in=visible_accounts)
        try:
            item = queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        filters = self.get_filters()
        accounts = []
        if item.account:
            accounts.append(item.account.id)
        show_conversions = request.user.has_permission(StaticPermissions.MANAGED_SERVICE__CONVERSIONS)
        show_aw_costs = request.user.has_permission(StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST)
        chart = DeliveryChart(
            accounts=accounts,
            dimension=dimension,
            show_conversions=show_conversions,
            show_aw_costs=show_aw_costs,
            **filters)
        data = chart.get_items()
        data["currency_code"] = get_currency_code(item, show_aw_costs)
        managed_service_hide_delivery_data = not request.user.has_permission(
            StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS)
        if managed_service_hide_delivery_data:
            # These fields cannot be removed in base classes, because
            # the fields are used to calc extra params CPM, CTR, *rates, etc.
            for item in data['items']:
                for field in MANAGED_SERVICE_DELIVERY_DATA:
                    item[field] = None
            for field in MANAGED_SERVICE_DELIVERY_DATA:
                data['summary'][field] = None
        return Response(data=data)
