from django.db.models import Sum
from rest_framework.response import Response

from aw_reporting.api.views.base_track import TrackApiBase
from aw_reporting.demo import demo_view_decorator
from aw_reporting.models import Account


@demo_view_decorator
class TrackFiltersListApiView(TrackApiBase):
    """
    Lists of the filter names and values
    """

    def get_static_filters(self):
        static_filters = dict(
            indicator=[
                dict(id=uid, name=name)
                for uid, name in self.indicators
            ],
            breakdown=[
                dict(id=uid, name=name)
                for uid, name in self.breakdowns
            ],
            dimension=[
                dict(id=uid, name=name)
                for uid, name in self.dimensions
            ],
        )
        return static_filters

    def get(self, request, *args, **kwargs):
        accounts = Account.user_objects(request.user).filter(
            can_manage_clients=False,
        ).annotate(
            impressions=Sum("campaigns__impressions")
        ).filter(impressions__gt=0).distinct()

        filters = dict(
            accounts=[
                dict(
                    id=account.id,
                    name=account.name,
                    start_date=account.start_date,
                    end_date=account.end_date,
                    campaigns=[
                        dict(
                            id=c.id,
                            name=c.name,
                            start_date=c.start_date,
                            end_date=c.end_date,
                        )
                        for c in account.campaigns.all()
                    ]
                )
                for account in accounts
            ],
            **self.get_static_filters()
        )
        return Response(data=filters)
