from aw_reporting.api.views.trends.base_global_trends import get_account_queryset
from aw_reporting.api.views.trends.base_global_trends import get_filters
from aw_reporting.api.views.trends.base_track_chart import BaseTrackChartApiView


class GlobalTrendsChartsApiView(BaseTrackChartApiView):
    def _get_accounts(self, request):
        return get_account_queryset(request.user)

    def get_filters(self):
        filters = super(GlobalTrendsChartsApiView, self).get_filters()
        global_filters = get_filters(self.request)

        return dict(
            **filters,
            **global_filters,
            with_plan=True,
        )
