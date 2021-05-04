import datetime
from unittest import mock

from oauth.constants import OAuthData
from oauth.models import OAuthAccount
from performiq.models.constants import OAuthType
from utils.unittests.test_case import ExtendedAPITestCase
from utils.datetime import now_in_default_tz
from oauth.tasks.segment_gads_oauth_notify import segment_gads_oauth_notify_task
from oauth.tasks.segment_gads_oauth_notify import NOTIFY_HOURS_THRESHOLD


class SegmentGadsOAuthNotifyTestCase(ExtendedAPITestCase):
    def setUp(self) -> None:
        self.user = self.create_test_user()

    def test_sends_notify_email(self):
        """ Test sends notify email if OAuthAccount has not finished oauth process and time elapsed
        exceeds NOTIFY_HOURS_THRESHOLD """
        oauth_data = {
            OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP: str(
                now_in_default_tz() - datetime.timedelta(hours=NOTIFY_HOURS_THRESHOLD + 1))
        }
        OAuthAccount.objects.create(
            oauth_type=int(OAuthType.GOOGLE_ADS), is_enabled=True, synced=True, data=oauth_data, user=self.user
        )
        with mock.patch("oauth.tasks.segment_gads_oauth_notify._send_email") as mock_send_email:
            segment_gads_oauth_notify_task()
        mock_send_email.assert_called_once()

    def test_ignore_email(self):
        """ Test should not send notify email if time has not exceeded threshold hours """
        oauth_account = OAuthAccount.objects.create(user=self.user, oauth_type=int(OAuthType.GOOGLE_ADS),
                                                    is_enabled=True, synced=True)
        with self.subTest("Test should not send notify email if oauth account has not started Google Ads Oauth process"):
            # oauth_account has no OAuthData.GADS_OAUTH_TIMESTAMP timestamp and should be ignored
            with mock.patch("oauth.tasks.segment_gads_oauth_notify._send_email") as mock_send_email:
                segment_gads_oauth_notify_task()
            mock_send_email.assert_not_called()

        with self.subTest("Test should not send notify email if time has not exceeded threshold hours"):
            oauth_data = {
                OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP: str(
                    now_in_default_tz() - datetime.timedelta(hours=NOTIFY_HOURS_THRESHOLD - 1))
            }
            OAuthAccount.objects.filter(id=oauth_account.id).update(data=oauth_data)
            with mock.patch("oauth.tasks.segment_gads_oauth_notify._send_email") as mock_send_email:
                segment_gads_oauth_notify_task()
            mock_send_email.assert_not_called()

        with self.subTest("Test should not send notify email if OAuthAccount has successfully been synced"):
            # Value of True indicates OAuthAccount has been synced in SegmentGadsSyncAPIView get method
            oauth_data = {OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP: True}
            OAuthAccount.objects.filter(id=oauth_account.id).update(data=oauth_data)
            with mock.patch("oauth.tasks.segment_gads_oauth_notify._send_email") as mock_send_email:
                segment_gads_oauth_notify_task()
            mock_send_email.assert_not_called()

    def test_notification_once(self):
        """ Test that notification email is only sent once """
        oauth_data = {
            OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP: str(
                now_in_default_tz() - datetime.timedelta(hours=NOTIFY_HOURS_THRESHOLD + 1))
        }
        OAuthAccount.objects.create(
            oauth_type=int(OAuthType.GOOGLE_ADS), is_enabled=True, synced=True, data=oauth_data, user=self.user
        )
        with mock.patch("oauth.tasks.segment_gads_oauth_notify._send_email") as mock_first_send_email:
            segment_gads_oauth_notify_task()
        mock_first_send_email.assert_called_once()

        with mock.patch("oauth.tasks.segment_gads_oauth_notify._send_email") as mock_second_send_email:
            segment_gads_oauth_notify_task()
        mock_second_send_email.assert_not_called()

