from datetime import datetime

from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.charts import DeliveryChart
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import DATE_FORMAT


@demo_view_decorator
class AnalyticsPerformanceChartItemsApiView(APIView):
    """
    Send filters to get a list of targeted items

    Body example:

    {"segmented": false}
    """
    permission_classes = (IsAuthenticated,)

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
        user = request.user
        try:
            item = AccountCreation.objects.user_related(user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        filters = self.get_filters()
        accounts = []
        if item.account:
            accounts.append(item.account.id)
        chart = DeliveryChart(
            accounts=accounts,
            dimension=dimension,
            always_aw_costs=True,
            **filters)
        data = chart.get_items()
        return Response(data=data)
