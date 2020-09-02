""" Update individual Google Ads CID accounts """
import logging

from django.db.models import Sum

from aw_reporting.adwords_reports import account_performance
from aw_reporting.models import Account
from aw_reporting.update.adwords_utils import get_base_stats

logger = logging.getLogger(__name__)


class AccountUpdater:
    def __init__(self, account):
        self._quartile_rate_fields = [f"video_views_{rate}_quartile" for rate in ("25", "50", "75", "100")]
        self.account = account

    def update(self, client):
        try:
            predicates = [
                {"field": "ExternalCustomerId", "operator": "IN", "values": self.account.id},
            ]
            aggregated_stats_mapping = self._get_aggregated_stats()
            report = account_performance(client, predicates=predicates)
            account = self._set_stats(report, aggregated_stats_mapping)
            account.save()
        except IndexError:
            logger.warning(f"Unable to get stats for cid: {self.account.id}")

    def _set_stats(self, report, aggregated_stats):
        row = report[0]
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
        [
            setattr(self.account, key, value) for key, value in stats.items()
        ]
        return self.account

    def _get_aggregated_stats(self):
        queryset = Account.objects.filter(id__in=[self.account.id]).values("id")
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

