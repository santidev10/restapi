import mock

from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.tasks.constants import Schedulers
from performiq.tasks.google_ads_scheduler import google_ads_update_scheduler
from utils.unittests.test_case import ExtendedAPITestCase



class GAdsUpdateSchedulerTestCase(ExtendedAPITestCase):
    def test_lock_no_schedule(self):
        """ Test that scheduler should not schedule account update if it is already updating """
        user = self.create_test_user()
        OAuthAccount.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)
        with mock.patch("performiq.tasks.google_ads_scheduler.get_lock", return_value=("", False)),\
            mock.patch("performiq.tasks.google_ads_scheduler.update_campaigns_task.delay") as mock_task:
                google_ads_update_scheduler.run()
                mock_task.assert_not_called()

    def test_schedule_success(self):
        """ Test scheduler successfully schedules update task"""
        user = self.create_test_user()
        OAuthAccount.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)
        with mock.patch("performiq.tasks.google_ads_scheduler.get_lock", return_value=("", True)), \
             mock.patch("performiq.tasks.google_ads_scheduler.update_campaigns_task.delay") as mock_task:
                google_ads_update_scheduler.run()
                mock_task.assert_called_once()

    def test_queue_limit(self):
        """ Test that scheduler does not fill queue past limit """
        user = self.create_test_user()
        accounts = [
            OAuthAccount(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)
            for _ in range(Schedulers.GoogleAdsUpdateScheduler.MAX_QUEUE_SIZE)
        ]
        OAuthAccount.objects.bulk_create(accounts)
        mock_queue_size_val = 7
        with mock.patch("performiq.tasks.google_ads_scheduler.get_queue_size", return_value=mock_queue_size_val),\
            mock.patch("performiq.tasks.google_ads_scheduler.update_campaigns_task.delay") as mock_task,\
            mock.patch("performiq.tasks.google_ads_scheduler.get_lock", return_value=("", True)):
            google_ads_update_scheduler.run()
            expected_call_count = Schedulers.GoogleAdsUpdateScheduler.MAX_QUEUE_SIZE - mock_queue_size_val
            # Scheduler should only fill queue up to the max queue size
            self.assertEqual(mock_task.call_count, expected_call_count)
