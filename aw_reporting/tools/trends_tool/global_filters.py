from aw_reporting.api.views.trends.base_global_trends import get_account_queryset
from aw_reporting.calculations.territories import get_salesforce_territories
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import User
from aw_reporting.models import goal_type_str

from aw_reporting.tools.trends_tool.base_filters import BaseTrackFiltersList


class GlobalTrendsFilters(BaseTrackFiltersList):
    def _get_accounts(self, user):
        return get_account_queryset(user)

    def _get_static_filters(self):
        static_filters = super(GlobalTrendsFilters, self)._get_static_filters()
        territories = get_salesforce_territories()

        return dict(
            goal_types=[dict(id=t, name=goal_type_str(t))
                        for t in sorted([SalesForceGoalType.CPM,
                                         SalesForceGoalType.CPV])],
            region=[dict(id=territory, name=territory) for territory in territories],
            **static_filters
        )

    def get_filters(self, user, **kwargs):
        accounts = self._get_accounts(user)
        accounts = accounts.exclude(
            campaigns__impressions=0,
            campaigns__video_views=0,
            campaigns__cost=0
        ) \
            .distinct()
        base_filters = super(GlobalTrendsFilters, self).get_filters(user, accounts)

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
    filters["is_active"] = True
    users = User.objects.filter(**filters) \
        .distinct()
    return [dict(id=am.id, name=am.name) for am in users]


def _map_items(items):
    return [dict(id=i, name=i) for i in items]
