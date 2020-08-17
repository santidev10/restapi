""" Update individual Google Ads CID accounts """
from aw_reporting.adwords_reports import account_performance
from aw_reporting.models import Account


class AccountUpdater:
    def __init__(self, account_ids):
        self.account_ids = [account_ids] if not isinstance(account_ids, list) else account_ids
        self.fields_mapping = {
            "ExternalCustomerId": ("id", None),
            "Clicks": ("clicks", int),
            "VideoViews": ("video_views", int),
            "Impressions": ("impressions", int),
            "Cost": ("cost", float),
            "ActiveViewViewability": ("active_view_viewability", float),
        }

    def update(self, client):
        try:
            predicates = [
                {"field": "ExternalCustomerId", "operator": "IN", "values": self.account_ids},
            ]
            report = account_performance(client, predicates=predicates)
            accounts = self._get_stats(report)
            Account.objects.bulk_update(accounts, fields=[field[0] for field in self.fields_mapping.keys()])
        except IndexError:
            pass

    def _get_stats(self, report):
        for row in report:
            stats = {}
            for field in self.fields_mapping.keys():
                value = getattr(row, field)
                if field == "ActiveViewViewability":
                    value = value.strip("%")
                model_field, converter = self.fields_mapping[field]
                if converter:
                    value = converter(value)
                stats[model_field] = value
            yield Account(**stats)
