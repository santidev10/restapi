from datetime import datetime

from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_reporting.analytics_charts import DeliveryChart
from aw_reporting.demo.decorators import demo_view_decorator
from userprofile.models import UserSettingsKey
from aw_reporting.models import Account
from aw_reporting.models import DATE_FORMAT


@demo_view_decorator
class AnalyzeChartApiView(APIView):
    """
    Send filters to get data for charts

    Body example:

    {"indicator": "impressions", "dimension": "device"}
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
            indicator=data.get("indicator", "average_cpv"),
            dimension=data.get("dimension"),
        )
        return filters

    def post(self, request, pk, **_):
        try:
            item = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        filters = self.get_filters()
        show_aw_costs = request.user.get_aw_settings().get(UserSettingsKey.DASHBOARD_AD_WORDS_RATES)
        chart = DeliveryChart([item.id], segmented_by="campaigns", show_aw_costs=show_aw_costs, **filters)
        chart_data = chart.get_response()
        return Response(data=chart_data)
