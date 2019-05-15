from rest_framework.response import Response

from aw_reporting.api.views.trends.base_track import TrackApiBase


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
                self._map_account(account)
                for account in accounts
            ],
            **self._get_static_filters()
        )

    def _map_account(self, account):
        campaigns = list(account.campaigns.all())
        starts = [c.start_date for c in campaigns]
        ends = [c.end_date for c in campaigns]
        return dict(
            id=account.id,
            name=account.name,
            start_date=min(filter(lambda x: x is not None, starts)) if any(starts) else None,
            end_date=max(filter(lambda x: x is not None, ends)) if any(ends) else None,
            campaigns=[
                self._map_campaigns(campaign)
                for campaign in campaigns
            ]
        )

    def _map_campaigns(self, campaign):
        return dict(
            id=campaign.id,
            name=campaign.name,
            start_date=campaign.start_date,
            end_date=campaign.end_date,
        )

    def get(self, request, *args, **kwargs):
        filters = self._get_filters(request)
        return Response(data=filters)
