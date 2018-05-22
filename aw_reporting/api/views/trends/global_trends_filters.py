from aw_reporting.api.views.trends.base_global_trends import \
    get_account_queryset
from aw_reporting.api.views.trends.base_track_filter_list import \
    BaseTrackFiltersListApiView
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import User, Opportunity, goal_type_str, \
    SalesForceGoalType
from aw_reporting.models.salesforce_constants import ALL_SALESFORCE_REGIONS, \
    salesforce_region_str


@demo_view_decorator
class GlobalTrendsFiltersApiView(BaseTrackFiltersListApiView):
    def _get_accounts(self, request):
        return get_account_queryset()

    def _get_static_filters(self):
        static_filters = super(GlobalTrendsFiltersApiView,
                               self)._get_static_filters()
        return dict(
            goal_types=[dict(id=t, name=goal_type_str(t))
                        for t in sorted([SalesForceGoalType.CPM,
                                         SalesForceGoalType.CPV])],
            region=[dict(id=region_id, name=salesforce_region_str(region_id))
                    for region_id in ALL_SALESFORCE_REGIONS],
            **static_filters
        )

    def _get_filters(self, request):
        base_filters = super(GlobalTrendsFiltersApiView, self) \
            ._get_filters(request)
        accounts = self._get_accounts(request)
        accounts = accounts.filter(campaigns__statistics__isnull=False)\
                           .distinct()

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
