from aw_reporting.api.views.trends.base_global_trends import \
    get_account_queryset
from aw_reporting.api.views.trends.base_track_data import BaseTrackDataApiView


class GlobalTrendsDataApiView(BaseTrackDataApiView):

    def _get_accounts(self, request):
        return get_account_queryset()
