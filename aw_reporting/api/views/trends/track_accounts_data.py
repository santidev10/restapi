from aw_reporting.api.views.trends.base_track_data import BaseTrackDataApiView
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import Account


@demo_view_decorator
class TrackAccountsDataApiView(BaseTrackDataApiView):

    def _get_accounts(self, request):
        return Account.user_objects(request.user).filter(
            can_manage_clients=False,
        )