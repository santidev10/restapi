from django.urls import reverse
from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_200_OK

from oauth.api.urls.names import OAuthPathName
from oauth.constants import OAuthData
from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase
from utils.datetime import now_in_default_tz


class OAuthAccountsAPITestCase(ExtendedAPITestCase):
    def _get_url(self):
        return reverse(Namespace.OAUTH + ":" + OAuthPathName.OAUTH_ACCOUNT_LIST_UPDATE)

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user()
        self.gads_oauth = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.GOOGLE_ADS.value,
                                                      email="gads_oauth@gmail.com")
        self.dv360_oauth = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.DV360.value,
                                                       email="dv360_oauth@gmail.com")

    def test_get_success(self):
        user2 = get_user_model().objects.create(email="test@tester.com")
        OAuthAccount.objects.create(user=user2, oauth_type=OAuthType.GOOGLE_ADS.value)
        OAuthAccount.objects.create(user=user2, oauth_type=OAuthType.DV360.value)
        response = self.client.get(self._get_url())
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["gads"]["email"], self.gads_oauth.email)
        self.assertEqual(data["dv360"]["email"], self.dv360_oauth.email)

    def test_segment_gads_oauth_verified(self):
        """ Test OAuthAccount segment_sync_script_is_verified serialization value """
        with self.subTest("Test segment_sync_script_is_verified is False no timestamp"):
            # self.gads_oauth.data has no OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP value
            response = self.client.get(self._get_url())
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["gads"]["segment_sync_script_is_verified"], False)

        with self.subTest("Test segment_sync_script_is_verified is False with timestamp"):
            # self.gads_oauth.data OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP has str timestamp value, pending Google Ads sync
            self.gads_oauth.data.update({
                OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP: str(now_in_default_tz()),
            })
            self.gads_oauth.save(update_fields=["data"])
            response = self.client.get(self._get_url())
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["gads"]["segment_sync_script_is_verified"], False)

        with self.subTest("Test segment_sync_script_is_verified is True"):
            # self.gads_oauth.data OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP has True value, indicating successful sync
            self.gads_oauth.data.update({
                OAuthData.SEGMENT_GADS_OAUTH_TIMESTAMP: True,
            })
            self.gads_oauth.save(update_fields=["data"])
            response = self.client.get(self._get_url())
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data["gads"]["segment_sync_script_is_verified"], True)
