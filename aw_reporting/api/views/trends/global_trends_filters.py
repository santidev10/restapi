from aw_reporting.api.views.trends.base_track_filter_list import \
    BaseTrackFiltersListApiView
from aw_reporting.api.constants import \
    GEO_LOCATION_TIP, GEO_LOCATION_CONDITION
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import Account, User, Opportunity, goal_type_str, \
    SalesForceGoalType
from aw_reporting.settings import InstanceSettings, InstanceSettingsKey


@demo_view_decorator
class GlobalTrendsFiltersApiView(BaseTrackFiltersListApiView):
    def _get_accounts(self, request):
        global_trends_accounts_id = InstanceSettings() \
            .get(InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS)
        return Account.objects \
            .filter(managers__id__in=global_trends_accounts_id)

    def _get_static_filters(self):
        static_filters = super(GlobalTrendsFiltersApiView,
                               self)._get_static_filters()
        return dict(
            goal_types=[dict(id=t, name=goal_type_str(t))
                        for t in sorted([SalesForceGoalType.CPM,
                                         SalesForceGoalType.CPV])],
            geo_locations=GEO_LOCATION_TIP,
            geo_locations_condition=GEO_LOCATION_CONDITION,
            **static_filters
        )

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
        opportunities = Opportunity.objects.filter(
            placements__adwords_campaigns__account__in=accounts,
        ) \
            .distinct()
        brands = opportunities.filter(brand__isnull=False) \
            .values_list("brand", flat=True) \
            .order_by("brand")
        categories = opportunities.filter(category__isnull=False) \
            .values_list("category_id", flat=True) \
            .order_by("category_id")
        return dict(
            am=am_data,
            ad_ops=ad_ops_data,
            sales=sales_data,
            brands=_map_items(brands),
            categories=_map_items(categories),
            **base_filters,
        )


def _users_data(**filters):
    users = User.objects.filter(**filters) \
        .distinct()
    return [dict(id=am.id, name=am.name) for am in users]


def _map_items(items):
    return [dict(id=i, name=i) for i in items]
