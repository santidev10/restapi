from datetime import datetime

from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_reporting.analytics_charts import DeliveryChart
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import DATE_FORMAT, Account


@demo_view_decorator
class AnalyzeChartItemsApiView(APIView):
    """
    Send filters to get a list of targeted items

    Body example:

    {"segmented": false}
    """

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
            segmented_by=data.get("segmented"),
        )
        return filters

    def post(self, request, pk, **kwargs):
        dimension = kwargs.get("dimension")
        try:
            item = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        chart = DeliveryChart(
            accounts=[item.id],
            dimension=dimension,
            **filters
        )
        items = chart.get_items()
        return Response(data=items)