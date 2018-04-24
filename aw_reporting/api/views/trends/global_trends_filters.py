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
        am_data = _users_data(
            managed_opportunities__placements__adwords_campaigns__account__in=self._get_accounts(request)
        )
        ad_ops_data = _users_data(
            ad_managed_opportunities__placements__adwords_campaigns__account__in=self._get_accounts(
                request)
        )
        return dict(
            am=am_data,
            ad_ops=ad_ops_data,
            sales=[],
            brands=[],
            goal_types=[],
            verticals=[],
            regions=[],
            **base_filters,
        )


def _users_data(**filters):
    users = User.objects.filter(**filters) \
        .distinct()
    return [dict(id=am.id, name=am.name) for am in users]
