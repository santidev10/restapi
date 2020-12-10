import datetime

from performiq.models import Account
from performiq.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.tasks.update_campaigns import _get_cids_to_update
from performiq.tasks.update_campaigns import GADS_CID_UPDATE_THRESHOLD
from utils.unittests.test_case import ExtendedAPITestCase
from utils.datetime import now_in_default_tz


class GAdsCampaignUpdateTestCase(ExtendedAPITestCase):
    def test_ignore_recently_updated_accounts(self):
        """ Test that the same Google Ads Account's are not updated multiple times since an Account can be linked to
        multiple OAuthAccount's. """
        user = self.create_test_user()
        oauth_account = OAuthAccount.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)
        account = Account.objects.create(oauth_account=oauth_account)
        to_update = _get_cids_to_update([account.id])
        self.assertEqual(len(to_update), 0)

    def test_get_cids_to_update(self):
        user = self.create_test_user()
        oauth_account = OAuthAccount.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)
        account = Account.objects.create(oauth_account=oauth_account)
        outdated = now_in_default_tz() - datetime.timedelta(minutes=GADS_CID_UPDATE_THRESHOLD + 1)
        Account.objects.filter(id=account.id).update(updated_at=outdated)
        to_update = _get_cids_to_update([account.id])
        self.assertEqual(len(to_update), 1)
        self.assertEqual(to_update[0], account.id)
