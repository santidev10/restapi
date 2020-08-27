""" Update individual Google Ads CID accounts """
from django.db.models import Sum

from aw_reporting.adwords_reports import account_performance
from aw_reporting.models import Account
from aw_reporting.update.adwords_utils import get_base_stats


class AccountUpdater:
    def __init__(self, account_ids):
        self.account_ids = [account_ids] if not isinstance(account_ids, list) else account_ids
        self._quartile_rate_fields = [f"video_views_{rate}_quartile" for rate in  ("25", "50", "75", "100")]

    def update(self, client):
        try:
            predicates = [
                {"field": "ExternalCustomerId", "operator": "IN", "values": self.account_ids},
            ]
            aggregated_stats_mapping = self._get_aggregated_stats()
            report = account_performance(client, predicates=predicates)
            accounts = self._get_stats(report, aggregated_stats_mapping)
            Account.objects.bulk_update(accounts, fields=["clicks", "video_views", "impressions", "cost",
                                                          "active_view_viewability"] + self._quartile_rate_fields)

        except IndexError:
            pass

    def _get_stats(self, report, aggregated_stats):
        for row in report:
            stats = get_base_stats(row)
            account_id = int(row.ExternalCustomerId)
            try:
                active_view_viewability = float(row.ActiveViewViewability.strip("%"))
            except (ValueError, TypeError):
                active_view_viewability = 0
            stats.update({
                "id": account_id,
                "active_view_viewability": active_view_viewability,
                **aggregated_stats.get(account_id, {})
            })
            yield Account(**stats)

    def _get_aggregated_stats(self):
        queryset = Account.objects.filter(id__in=self.account_ids).values("id")
        self._stats_annotation = {
            **{
                quartile_field: Sum(f"campaigns__{quartile_field}")
                for quartile_field in self._quartile_rate_fields
            }
        }
        aggregated_stats = queryset.annotate(**self._stats_annotation)
        aggregated_stats_mapping = {
            stats.pop("id"): stats for stats in aggregated_stats
        }
        return aggregated_stats_mapping
