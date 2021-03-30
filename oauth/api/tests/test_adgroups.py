from django.urls import reverse
from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from oauth.api.urls.names import OAuthPathName
from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from oauth.utils.test import create_gads_resources
from oauth.utils.test import create_dv360_resources
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase


class OAuthAdgroupAPITestCase(ExtendedAPITestCase):
    def _get_url(self, oauth_type, parent_id=None):
        url = reverse(Namespace.OAUTH + ":" + OAuthPathName.ADGROUP_LIST) + f"?oauth_type={oauth_type}"
        if parent_id is not None:
            url += f"&parent_id={parent_id}"
        return url

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user()
        self.gads_oauth = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.GOOGLE_ADS.value,
                                                       email="gads_oauth@gmail.com")
        self.dv360_oauth = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.DV360.value,
                                                       email="dv360_oauth@gmail.com")
        self.account, self.gads_campaign, self.adgroup = create_gads_resources(self.gads_oauth)
        self.advertiser, self.dv_campaign, self.insertion_order = create_dv360_resources(self.dv360_oauth)

    def test_fail_oauth_type(self):
        """ Test invalid oauth type """
        response = self.client.get(self._get_url(-1))
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_empty_parent_wrong_oauth_type(self):
        """ Test empty results with filtering with parent id and wrong oauth type """
        with self.subTest("Incorrect oauth type with dv360 campaign parent filter"):
            response = self.client.get(self._get_url(OAuthType.GOOGLE_ADS.value, parent_id=self.dv_campaign.id))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data), 0)

        with self.subTest("Incorrect oauth type with gads campaign parent filter"):
            response = self.client.get(self._get_url(OAuthType.DV360.value, parent_id=self.gads_campaign.id))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data), 0)

    def test_get_gads_success(self):
        # Create other resources to test filtering
        _, _, adgroup2 = create_gads_resources(self.gads_oauth)

        other_user = get_user_model().objects.create(email="test@tester.com")
        other_oauth = OAuthAccount.objects.create(user=other_user, oauth_type=OAuthType.GOOGLE_ADS.value)
        _, _, other_adgroup = create_gads_resources(other_oauth)

        with self.subTest("Test getting all"):
            response = self.client.get(self._get_url(OAuthType.GOOGLE_ADS.value))
            self.assertEqual(response.status_code, HTTP_200_OK)
            data = sorted(response.data, key=lambda item: item["id"])
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0]["id"], self.adgroup.id)
            self.assertEqual(data[1]["id"], adgroup2.id)

        with self.subTest("Test getting with parent gads campaign id"):
            response = self.client.get(self._get_url(OAuthType.GOOGLE_ADS.value, parent_id=self.gads_campaign.id))
            data2 = response.data
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(data2), 1)
            self.assertEqual(data2[0]["id"], self.adgroup.id)
