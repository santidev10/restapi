from datetime import datetime

from django.http import Http404
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.api.serializers.common.utils import get_currency_code
from aw_creation.models import AccountCreation
from aw_reporting.charts.dashboard_charts import DeliveryChart
from aw_reporting.charts.dashboard_charts import Indicator
from aw_reporting.models import DATE_FORMAT
from userprofile.constants import StaticPermissions


class DashboardPerformanceChartApiView(APIView):
    """
    Send filters to get data for charts

    Body example:

    {"indicator": "impressions", "dimension": "device"}
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
            indicator=data.get("indicator", "average_cpv"),
            dimension=data.get("dimension"))
        return filters

    def post(self, request, pk, **_):
        self.filter_hidden_sections()
        filters = {}
        if not request.user.has_permission(StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS):
            filters["account__id__in"] = request.user.get_visible_accounts_list()
        try:
            item = AccountCreation.objects.filter(**filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        filters = self.get_filters()
        account_ids = []
        if item.account:
            account_ids.append(item.account.id)

        show_aw_costs = request.user.has_permission(StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST)
        chart = DeliveryChart(accounts=account_ids, segmented_by="campaigns",
                              show_aw_costs=show_aw_costs, **filters)
        chart_data = chart.get_response()
        currency_code = get_currency_code(item, show_aw_costs)
        for chart in chart_data:
            chart["currency_code"] = currency_code
        return Response(data=chart_data)

    def filter_hidden_sections(self):
        user = self.request.user
        if not user.has_permission(StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST):
            hidden_indicators = Indicator.CPV, Indicator.CPM
            if self.request.data.get("indicator") in hidden_indicators:
                raise Http404
