from django.db.models import Sum

from aw_reporting.api.views.trends.base_track_filter_list import \
    BaseTrackFiltersListApiView
from aw_reporting.models import Account


class TrackFiltersListApiView(BaseTrackFiltersListApiView):
    """
    Lists of the filter names and values
    """

    def _get_accounts(self, request):
        return Account.user_objects(request.user) \
            .filter(can_manage_clients=False, ) \
            .annotate(impressions=Sum("campaigns__impressions")) \
            .filter(impressions__gt=0) \
            .distinct()
