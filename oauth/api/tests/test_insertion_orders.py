from django.urls import reverse
from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_200_OK

from oauth.api.urls.names import OAuthPathName
from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from oauth.utils.test import create_dv360_resources
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase


class OAuthInsertionOrderAPITestCase(ExtendedAPITestCase):
    def _get_url(self):
        return reverse(Namespace.OAUTH + ":" + OAuthPathName.INSERTION_ORDER_LIST)

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user()
        self.dv360_oauth = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.DV360.value,
                                                       email="dv360_oauth@gmail.com")
        self.advertiser, self.campaign, self.insertion_order = create_dv360_resources(self.dv360_oauth)

    def test_get_success(self):
        # Create other resources to test filtering
        _, _, insertion_order2 = create_dv360_resources(self.dv360_oauth)

        user2 = get_user_model().objects.create(email="test@tester.com")
        oauth2 = OAuthAccount.objects.create(user=user2, oauth_type=OAuthType.DV360.value)
        _, _, other_insertion_order = create_dv360_resources(oauth2)

        with self.subTest("Test getting all"):
            response = self.client.get(self._get_url())
            self.assertEqual(response.status_code, HTTP_200_OK)
            data = sorted(response.data, key=lambda io: io["id"])
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0]["id"], self.insertion_order.id)
            self.assertEqual(data[1]["id"], insertion_order2.id)

        with self.subTest("Test getting with parent advertiser id"):
            response = self.client.get(self._get_url() + f"?parent_id={self.campaign.id}")
            data = response.data
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0]["id"], self.insertion_order.id)
