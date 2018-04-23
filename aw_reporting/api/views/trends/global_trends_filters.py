from aw_reporting.api.views.trends.base_track_filter_list import \
    BaseTrackFiltersListApiView
from aw_reporting.demo import demo_view_decorator
from aw_reporting.models import Account
from aw_reporting.settings import InstanceSettings, InstanceSettingsKey


@demo_view_decorator
class GlobalTrendsFiltersApiView(BaseTrackFiltersListApiView):
    def _get_accounts(self, request):
        global_trends_accounts_id = InstanceSettings() \
            .get(InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS)
        return Account.objects \
            .filter(managers__id__in=global_trends_accounts_id)
