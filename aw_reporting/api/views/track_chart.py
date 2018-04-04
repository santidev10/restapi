from rest_framework.response import Response

from aw_reporting.api.views.base_track import TrackApiBase
from aw_reporting.charts import DeliveryChart
from aw_reporting.demo import demo_view_decorator
from aw_reporting.models import Account


@demo_view_decorator
class TrackChartApiView(TrackApiBase):
    """
    Returns data we need to build charts
    """

    def get(self, request, *args, **kwargs):
        filters = self.get_filters()
        visible_accounts = Account.user_objects(request.user).filter(
            can_manage_clients=False,
        ).values_list("id", flat=True)
        chart = DeliveryChart(visible_accounts,
                              additional_chart=False,
                              **filters)
        return Response(data=chart.get_response())
