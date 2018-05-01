from rest_framework.response import Response

from aw_reporting.api.views.trends.base_track import TrackApiBase
from aw_reporting.charts import DeliveryChart


class BaseTrackChartApiView(TrackApiBase):
    """
    Returns data we need to build charts
    """

    def _get_accounts(self, request):
        raise NotImplementedError

    def get(self, request, *args, **kwargs):
        filters = self.get_filters()
        visible_accounts = self._get_accounts(request) \
            .values_list("id", flat=True)
        chart = DeliveryChart(
            visible_accounts,
            additional_chart=False,
            **filters
        )
        return Response(data=chart.get_response())
