from rest_framework.response import Response

from aw_reporting.api.views.trends.base_track import TrackApiBase
from aw_reporting.analytics_charts import DeliveryChart


class BaseTrackDataApiView(TrackApiBase):
    """
    Returns a list of accounts for the table below the chart
    """

    def _get_accounts(self, request):
        raise NotImplementedError

    def get(self, request, *args, **kwargs):
        filters = self.get_filters()
        visible_accounts = self._get_accounts(request) \
            .values_list("id", flat=True)

        if filters["accounts"] is not None:
            visible_accounts = visible_accounts\
                .filter(id__in=filters["accounts"])
        del filters["accounts"]

        chart = DeliveryChart(
            visible_accounts,
            additional_chart=False,
            always_aw_costs=True,
            **filters
        )
        data = chart.get_account_segmented_data()
        return Response(data=data)
