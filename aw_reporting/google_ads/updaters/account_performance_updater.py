""" Handle Account updates through Account Performance Report """
from aw_reporting.adwords_reports import account_performance


class AccountPerformanceUpdater:
    def __init__(self, account):
        self.account = account

    def update(self, client):
        try:
            data = account_performance(client)[0]
            self.account.active_view_viewability = float(data.ActiveViewViewability.strip("%"))
            self.account.save()
        except (IndexError, AttributeError):
            pass
