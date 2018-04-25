from aw_reporting.api.views.trends.base_global_trends import \
    get_account_queryset
from aw_reporting.api.views.trends.base_track_data import BaseTrackDataApiView


class GlobalTrendsDataApiView(BaseTrackDataApiView):

    def get_filters(self):
        filters = super().get_filters()
        data = self.request.query_params

        am = data.get("am")
        am_ids = am.split(",") if am is not None else []
        return dict(
            am_ids=am_ids,
            **filters
        )

    def _get_accounts(self, request):
        return get_account_queryset()
