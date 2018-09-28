from datetime import datetime

from django.http import Http404
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.dashboard_charts import DeliveryChart, Indicator
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import DATE_FORMAT
from userprofile.constants import UserSettingsKey
from utils.permissions import UserHasDashboardPermission


@demo_view_decorator
class DashboardPerformanceChartApiView(APIView):
    """
    Send filters to get data for charts

    Body example:

    {"indicator": "impressions", "dimension": "device"}
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
            indicator=data.get("indicator", "average_cpv"),
            dimension=data.get("dimension"))
        return filters

    def post(self, request, pk, **_):
        self.filter_hidden_sections()
        filters = {}
        user_settings = request.user.get_aw_settings()
        if not user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS):
            filters["account__id__in"] = \
                user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
        try:
            item = AccountCreation.objects.filter(**filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        filters = self.get_filters()
        account_ids = []
        if item.account:
            account_ids.append(item.account.id)

        show_aw_costs = user_settings.get(UserSettingsKey.DASHBOARD_AD_WORDS_RATES)
        chart = DeliveryChart(account_ids, segmented_by="campaigns",
                              show_aw_costs=show_aw_costs, **filters)
        chart_data = chart.get_response()
        return Response(data=chart_data)

    def filter_hidden_sections(self):
        user = self.request.user
        if not user.get_aw_settings() \
                .get(UserSettingsKey.DASHBOARD_AD_WORDS_RATES):
            hidden_indicators = Indicator.CPV, Indicator.CPM
            if self.request.data.get("indicator") in hidden_indicators:
                raise Http404
