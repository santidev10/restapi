from unittest import mock

from googleads.errors import GoogleAdsServerFault

from oauth.api.views.google_ads_oauth import GoogleAdsOAuthAPIView
from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from utils.unittests.test_case import ExtendedAPITestCase


class GoogleAdsOAuthAPIViewTestCase(ExtendedAPITestCase):
    def test_handler_reset_oauth(self):
        """ Test that if handler raises exception, OAuthAccount is enabled status is reset """
        view_class = GoogleAdsOAuthAPIView()
        user = self.create_test_user()
        oauth_account = OAuthAccount.objects.create(user=user, oauth_type=int(OAuthType.GOOGLE_ADS))
        with mock.patch("oauth.api.views.google_ads_oauth.get_accounts", side_effect=GoogleAdsServerFault(None)):
            view_class.handler(oauth_account)
        oauth_account.refresh_from_db()
        self.assertEqual(oauth_account.is_enabled, False)
