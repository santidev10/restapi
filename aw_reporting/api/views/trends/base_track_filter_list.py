from django.db.models import Sum
from rest_framework.response import Response

from aw_reporting.api.views.trends.base_track import TrackApiBase
from aw_reporting.models import Account


class BaseTrackFiltersListApiView(TrackApiBase):
    """
    Lists of the filter names and values
    """

    def _get_static_filters(self):
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

    def _get_accounts(self, request):
        raise NotImplemented

    def _get_filters(self, request, accounts=None):
        if accounts is None:
            accounts = self._get_accounts(request)

        return dict(
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
            **self._get_static_filters()
        )

    def get(self, request, *args, **kwargs):
        filters = self._get_filters(request)
        return Response(data=filters)
