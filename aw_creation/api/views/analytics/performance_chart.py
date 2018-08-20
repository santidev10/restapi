from datetime import datetime

from django.db.models import Q
from django.http import Http404

from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.charts import DeliveryChart
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import Account
from aw_reporting.models import DATE_FORMAT


@demo_view_decorator
class AnalyticsPerformanceChartApiView(APIView):
    """
    Send filters to get data for charts

    Body example:

    {"indicator": "impressions", "dimension": "device"}
    """
    permission_classes = (IsAuthenticated, )

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
        user = self.request.user
        related_accounts = Account.user_objects(user)
        queryset = AccountCreation.objects.filter(
            Q(is_deleted=False)
            & (Q(owner=user) | Q(account__in=related_accounts))
        )
        try:
            item = queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        filters = self.get_filters()
        account_ids = []
        if item.account:
            account_ids.append(item.account.id)
        chart = DeliveryChart(account_ids, segmented_by="campaigns",
                              always_aw_costs=True, **filters)
        chart_data = chart.get_response()
        return Response(data=chart_data)
