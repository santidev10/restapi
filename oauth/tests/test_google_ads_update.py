import datetime
from unittest import mock

from oauth.models import Account
from oauth.models import OAuthAccount
from performiq.models.constants import OAuthType
from oauth.tasks.google_ads_update import update_with_lock
from oauth.tasks.google_ads_update import google_ads_update_task
from oauth.tasks.google_ads_update import UPDATE_THRESHOLD
from utils.celery.tasks import REDIS_CLIENT
from utils.unittests.test_case import ExtendedAPITestCase
from utils.datetime import now_in_default_tz
from utils.unittests.patch_bulk_create import patch_bulk_create


@mock.patch("oauth.utils.adwords.safe_bulk_create", wraps=patch_bulk_create)
class GAdsUpdateSchedulerTestCase(ExtendedAPITestCase):
    def setUp(self):
        super().setUp()
        self.user = self.create_test_user()
        self.oauth_account = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.GOOGLE_ADS.value)

    def test_account_update_lock(self, *_):
        """ Test that accounts are updated only if a lock is acquired """
        mock_update = mock.MagicMock()
        mock_acquire = mock.MagicMock(side_effect=[True, False])
        mock_lock = mock.MagicMock(acquire=mock_acquire)
        account = Account.objects.create()
        # Mock unlock to simulate process already updating account
        with mock.patch("oauth.tasks.google_ads_update.unlock", mock.MagicMock),\
                mock.patch.object(REDIS_CLIENT, "lock", return_value=mock_lock):
            update_with_lock(mock_update, account.id, self.oauth_account)
            update_with_lock(mock_update, account.id, self.oauth_account)
        mock_update.assert_called_once()

    def test_account_update_outdated(self, *_):
        """ Test that account updated if outdated """
        outdated = now_in_default_tz() - datetime.timedelta(seconds=UPDATE_THRESHOLD * 2)
        account = Account.objects.create(id=12345)
        Account.objects.filter(id=account.id).update(updated_at=outdated)
        # First element in return value is list of mcc ids
        with mock.patch("oauth.tasks.google_ads_update.get_accounts", return_value=([dict(customerId=account.id)], [])),\
                mock.patch("oauth.tasks.google_ads_update.update_mcc_campaigns") as mock_mcc_update:
            google_ads_update_task([self.oauth_account.id])
        mock_mcc_update.assert_called_once()
        account.refresh_from_db()
        self.assertTrue(account.updated_at > outdated)

    def test_account_recently_updated_ignore(self, *_):
        """ Test that account is not updated if it is not outdated """
        account = Account.objects.create(id=12345, updated_at=now_in_default_tz())
        # First element in return value is list of mcc ids
        with mock.patch("oauth.tasks.google_ads_update.get_accounts", return_value=([dict(customerId=account.id)], [])),\
                mock.patch("oauth.tasks.google_ads_update.update_mcc_campaigns") as mock_mcc_update:
            google_ads_update_task([self.oauth_account.id])
        mock_mcc_update.assert_not_called()

    def test_non_existent_accounts_updated(self, *_):
        """ Test that accounts not in ViewIQ are updated """
        mock_account_id = "123456"
        with mock.patch("oauth.tasks.google_ads_update.get_accounts", return_value=([dict(customerId=mock_account_id)], [])),\
                mock.patch("oauth.tasks.google_ads_update.update_mcc_campaigns") as mock_mcc_update:
            google_ads_update_task([self.oauth_account.id])
        self.assertEqual(mock_mcc_update.call_args.args[0], mock_account_id)
