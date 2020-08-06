from datetime import timedelta
from mock import patch

from django.test import TransactionTestCase

from aw_reporting.models import Opportunity
from aw_reporting.models import OpPlacement
from aw_reporting.models import Campaign
from dashboard.tasks import update_opportunity_performance_task
from dashboard.tasks.update_opportunity_performance import MAX_HISTORY
from utils.unittests.int_iterator import int_iterator
from utils.datetime import now_in_default_tz


class TestUpdateOpportunityPerformance(TransactionTestCase):
    def _create_data(self):
        end = now_in_default_tz() + timedelta(days=10)
        op = Opportunity.objects.create(name="first", id=f"id_{next(int_iterator)}", probability=100, end=end)
        pl = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op)
        Campaign.objects.create(name="c", salesforce_placement=pl)
        return op

    def test_same_date_ignore(self):
        """ Test performance will not be updated multiple times per day """
        op = self._create_data()
        for _ in range(10):
            update_opportunity_performance_task()
        performance = op.performance.history
        self.assertEqual(len(performance), 1)

    def test_max_performance_size(self):
        """ Test performance will not be saved more than allowed max """
        op = self._create_data()
        today = now_in_default_tz()
        dates = [today + timedelta(days=i) for i in range(MAX_HISTORY + 5)]

        with patch("dashboard.tasks.update_opportunity_performance.now_in_default_tz") \
                as mock_today:
            mock_today.side_effect = dates
            for _ in range(len(dates)):
                update_opportunity_performance_task()
        performance = op.performance.history
        self.assertEqual(len(performance), MAX_HISTORY)
        self.assertEqual(performance[0]["date"], str(dates[-1].date()))
