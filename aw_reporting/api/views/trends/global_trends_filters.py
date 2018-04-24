from aw_reporting.api.views.trends.base_track_filter_list import \
    BaseTrackFiltersListApiView
from aw_reporting.demo import demo_view_decorator
from aw_reporting.models import Account, User
from aw_reporting.settings import InstanceSettings, InstanceSettingsKey


@demo_view_decorator
class GlobalTrendsFiltersApiView(BaseTrackFiltersListApiView):
    def _get_accounts(self, request):
        global_trends_accounts_id = InstanceSettings() \
            .get(InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS)
        return Account.objects \
            .filter(managers__id__in=global_trends_accounts_id)

    def _get_filters(self, request):
        base_filters = super(GlobalTrendsFiltersApiView, self) \
            ._get_filters(request)
        account_managers = User.objects.filter(
            managed_opportunities__placements__adwords_campaigns__account__in=self._get_accounts(
                request)) \
            .distinct()
        am_data = [dict(id=am.id, name=am.name) for am in account_managers]
        return dict(
            am=am_data,
            **base_filters,
        )
