from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from userprofile.constants import StaticPermissions
from utils.unittests.test_case import ExtendedAPITestCase


class CTLGadsScriptCodeTestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_GADS_SCRIPT)

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user(perms={
            StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST: True,
            StaticPermissions.BUILD__CTL_CREATE_VIDEO_LIST: True,
        })
        self.oauth_account = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.GOOGLE_ADS.value)

    def test_fail_not_oauthed(self):
        """ Test fails if user has not oauthed yet """
        self.oauth_account.delete()
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_get_success(self):
        """ Test that code is retrieved and VIQ_KEY is replaced with user viq_key """
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(str(self.oauth_account.viq_key) in response.data["code"])
