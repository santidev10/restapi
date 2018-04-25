from aw_reporting.api.views.trends.base_track_filter_list import \
    BaseTrackFiltersListApiView
from aw_reporting.demo import demo_view_decorator
from aw_reporting.models import Account, User, Opportunity
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
        accounts = self._get_accounts(request)
        am_data = _users_data(
            managed_opportunities__placements__adwords_campaigns__account__in=accounts
        )
        ad_ops_data = _users_data(
            ad_managed_opportunities__placements__adwords_campaigns__account__in=accounts
        )
        sales_data = _users_data(
            sold_opportunities__placements__adwords_campaigns__account__in=accounts
        )
        brands = Opportunity.objects.filter(
            placements__adwords_campaigns__account__in=accounts,
            brand__isnull=False,
        )\
            .values_list("brand", flat=True)\
            .order_by("brand")\
            .distinct()
        return dict(
            am=am_data,
            ad_ops=ad_ops_data,
            sales=sales_data,
            brands=[dict(id=b, name=b) for b in brands],
            goal_types=[],
            verticals=[],
            regions=[],
            **base_filters,
        )


def _users_data(**filters):
    users = User.objects.filter(**filters) \
        .distinct()
    return [dict(id=am.id, name=am.name) for am in users]
