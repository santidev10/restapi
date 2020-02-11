from itertools import groupby
from aw_reporting.models import Campaign
from aw_reporting.api.views.trends.constants import INDICATORS
from aw_reporting.api.views.trends.constants import BREAKDOWNS
from aw_reporting.api.views.trends.constants import DIMENSIONS


class BaseTrackFiltersList:
    """
    Lists of the filter names and values
    """

    def _get_static_filters(self):
        static_filters = dict(
            indicator=[
                dict(id=uid, name=name)
                for uid, name in INDICATORS
            ],
            breakdown=[
                dict(id=uid, name=name)
                for uid, name in BREAKDOWNS
            ],
            dimension=[
                dict(id=uid, name=name)
                for uid, name in DIMENSIONS
            ],
        )
        return static_filters

    def _get_accounts(self, user):
        raise NotImplemented

    def get_filters(self, user, accounts=None):
        if accounts is None:
            accounts = self._get_accounts(user)

        campaigns_map = self._get_campaigns_map()

        return dict(
            accounts=[
                self._map_account(account, campaigns_map.get(account.id, []))
                for account in accounts
            ],
            **self._get_static_filters()
        )

    def _map_account(self, account, campaigns):
        starts = [c["start_date"] for c in campaigns]
        ends = [c["end_date"] for c in campaigns]
        return dict(
            id=account.id,
            name=account.name,
            start_date=str(min(filter(lambda x: x is not None, starts))) if any(starts) else None,
            end_date=str(max(filter(lambda x: x is not None, ends))) if any(ends) else None,
            campaigns=[
                self._map_campaigns(campaign)
                for campaign in campaigns
            ]
        )

    def _get_campaigns_map(self):
        campaigns = Campaign.objects.all() \
            .values("id", "name", "start_date", "end_date", "account_id") \
            .order_by("account_id", "id")
        return {
            key: list(group)
            for key, group in groupby(campaigns, lambda i: i["account_id"])
        }

    def _map_campaigns(self, campaign):
        return dict(
            id=campaign["id"],
            name=campaign["name"],
            start_date=str(campaign["start_date"]),
            end_date=str(campaign["end_date"]),
        )
