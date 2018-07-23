from aw_reporting.api.views.trends.base_track_chart import BaseTrackChartApiView
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import Account


@demo_view_decorator
class TrackChartApiView(BaseTrackChartApiView):
    """
    Returns data we need to build charts
    """

    def _get_accounts(self, request):
        return Account.user_objects(request.user).filter(
            can_manage_clients=False,
        )

    def get_filters(self):
        filters = super(TrackChartApiView, self).get_filters()
        filters["with_plan"] = False
        return filters
