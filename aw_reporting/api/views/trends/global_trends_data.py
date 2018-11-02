from aw_reporting.api.views.trends.base_global_trends import \
    get_account_queryset, get_filters
from aw_reporting.api.views.trends.base_track_data import BaseTrackDataApiView


class GlobalTrendsDataApiView(BaseTrackDataApiView):

    def _get_accounts(self, request):
        return get_account_queryset(request.user)

    def get_filters(self):
        filters = super().get_filters()
        global_filters = get_filters(self.request)

        return dict(
            **filters,
            **global_filters
        )
