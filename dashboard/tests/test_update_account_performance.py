from datetime import timedelta
from mock import patch

from django.test import TestCase

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from dashboard.tasks import update_account_performance_task
from dashboard.tasks.update_opportunity_performance import MAX_HISTORY
from utils.unittests.int_iterator import int_iterator
from utils.datetime import now_in_default_tz


class TestUpdateAccountPerformance(TestCase):
    def _create_data(self):
        end = now_in_default_tz() + timedelta(days=100)
        account = Account.objects.create(name="first", id=next(int_iterator))
        op = Opportunity.objects.create()
        pl = OpPlacement.objects.create(opportunity=op, end=end)
        Campaign.objects.create(name="c", account=account, salesforce_placement=pl)
        return account

    def test_same_date_ignore(self):
        """ Test performance will not be updated multiple times per day """
        account = self._create_data()
        for _ in range(10):
            update_account_performance_task()
        performance = account.performance.history
        self.assertEqual(len(performance), 1)

    def test_max_performance_size(self):
        """ Test performance will not be saved more than allowed max """
        account = self._create_data()
        today = now_in_default_tz()
        dates = [today + timedelta(days=i) for i in range(MAX_HISTORY + 5)]

        with patch("dashboard.tasks.update_account_performance.now_in_default_tz") \
                as mock_today:
            mock_today.side_effect = dates
            for _ in range(len(dates)):
                update_account_performance_task()
        performance = account.performance.history
        self.assertEqual(len(performance), MAX_HISTORY)
        self.assertEqual(performance[-1]["date"], str(dates[-1].date()))
