from django.db.models import Q, Count, Min, Max
from rest_framework.generics import ListAPIView

from aw_reporting.api.serializers import AccountsListSerializer
from aw_reporting.api.views.pagination import AccountsListPaginator
from aw_reporting.demo import demo_view_decorator
from aw_reporting.models import Account, ConcatAggregate


@demo_view_decorator
class AnalyzeAccountsListApiView(ListAPIView):
    """
    Returns a list of user"s accounts that were pulled from AdWords
    """

    serializer_class = AccountsListSerializer
    pagination_class = AccountsListPaginator

    def get_queryset(self):
        queryset = Account.user_objects(self.request.user).filter(
            can_manage_clients=False,
        ).order_by("name", "id")
        return queryset

    filters = ("status", "search", "min_goal_units", "max_goal_units",
               "min_campaigns_count", "max_campaigns_count", "is_changed",
               "min_start", "max_start", "min_end", "max_end")

    def get_filters(self):
        filters = {}
        query_params = self.request.query_params
        for f in self.filters:
            v = query_params.get(f)
            if v:
                filters[f] = v
        return filters

    def filter_queryset(self, queryset):

        show_closed = self.request.query_params.get("show_closed")
        if not show_closed or not int(show_closed):
            queryset = queryset.annotate(
                statuses=ConcatAggregate("campaigns__status", distinct=True)
            ).exclude(
                ~Q(statuses__isnull=True) &
                Q(statuses__contains="ended") &
                ~Q(statuses__contains="eligible") &
                ~Q(statuses__contains="pending") &
                ~Q(statuses__contains="suspended")
            )

        filters = self.get_filters()
        search = filters.get("search")
        if search:
            queryset = queryset.filter(name__icontains=search)

        min_campaigns_count = filters.get("min_campaigns_count")
        max_campaigns_count = filters.get("max_campaigns_count")
        if min_campaigns_count or max_campaigns_count:
            queryset = queryset.annotate(campaigns_count=Count("campaigns"))
            if min_campaigns_count:
                queryset = queryset.filter(
                    campaigns_count__gte=min_campaigns_count)
            if max_campaigns_count:
                queryset = queryset.filter(
                    campaigns_count__lte=max_campaigns_count)

        queryset = queryset.annotate(start=Min("campaigns__start_date"),
                                     end=Max("campaigns__end_date"))

        min_start = filters.get("min_start")
        max_start = filters.get("max_start")
        if min_start or max_start:
            if min_start:
                queryset = queryset.filter(start__gte=min_start)
            if max_start:
                queryset = queryset.filter(start__lte=max_start)

        min_end = filters.get("min_end")
        max_end = filters.get("max_end")
        if min_end or max_end:
            if min_end:
                queryset = queryset.filter(end__gte=min_end)
            if max_end:
                queryset = queryset.filter(end__lte=max_end)

        return queryset
