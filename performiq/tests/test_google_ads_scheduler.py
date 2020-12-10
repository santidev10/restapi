import datetime
from unittest import mock

from django.utils import timezone

from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.tasks.google_ads_scheduler import google_ads_update_scheduler
from performiq.tasks.google_ads_scheduler import UPDATE_THRESHOLD
from utils.unittests.test_case import ExtendedAPITestCase


class GAdsUpdateSchedulerTestCase(ExtendedAPITestCase):
    def test_lock_no_schedule(self):
        """ Test that scheduler should not schedule account update if it is already updating
            i.e. get_lock returns False """
        user = self.create_test_user()
        OAuthAccount.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)
        with mock.patch("performiq.tasks.google_ads_scheduler.get_lock", return_value=("", False)), \
             mock.patch("performiq.tasks.google_ads_scheduler.update_campaigns_task.delay") as mock_task:
                google_ads_update_scheduler.run()
                mock_task.assert_not_called()

    def test_schedule_success(self):
        """ Test scheduler successfully schedules update task"""
        user = self.create_test_user()
        OAuthAccount.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)
        with mock.patch("performiq.tasks.google_ads_scheduler.get_lock", return_value=("", True)), \
             mock.patch("performiq.tasks.google_ads_scheduler.UPDATE_THRESHOLD", 0), \
             mock.patch("performiq.tasks.google_ads_scheduler.update_campaigns_task") as mock_task:
                google_ads_update_scheduler.run()
                mock_task.assert_called_once()

    def test_oauth_account_update_interval(self):
        """ Test that accounts should be updated only if last updated time is greater than threshold """
        user = self.create_test_user()
        now = timezone.now()
        expired = now - datetime.timedelta(minutes=UPDATE_THRESHOLD + 1)
        should_update = OAuthAccount.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)
        OAuthAccount.objects.filter(id=should_update.id).update(updated_at=expired)
        should_not_update = OAuthAccount.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)
        with mock.patch("performiq.tasks.google_ads_scheduler.update_campaigns_task") as mock_task, \
                mock.patch("performiq.tasks.google_ads_scheduler.get_lock", return_value=("", True)):
            google_ads_update_scheduler.run()
        should_update.refresh_from_db()
        should_not_update.refresh_from_db()
        call_args = mock_task.call_args[0]
        self.assertEqual(len(call_args), 1)
        self.assertEqual(call_args[0], should_update.id)
