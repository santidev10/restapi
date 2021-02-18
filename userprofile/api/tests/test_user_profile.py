import json
from mock import patch

from botocore.exceptions import ClientError
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED

from saas.urls.namespaces import Namespace
from userprofile.constants import DEFAULT_DOMAIN
from userprofile.models import WhiteLabel
from userprofile.api.urls.names import UserprofilePathName
from utils.unittests.test_case import ExtendedAPITestCase


class UserProfileTestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.USER_PROFILE + ":" + UserprofilePathName.USER_PROFILE)

    def setUp(self):
        _, _ = WhiteLabel.objects.get_or_create(domain=DEFAULT_DOMAIN)

    def _update(self, data):
        return self.client.put(self._url, json.dumps(data),
                               content_type="application/json")

    def test_require_auth(self):
        response = self._update(dict())

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_success(self):
        self.create_test_user()
        response = self._update(dict())
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                "aw_settings",
                "can_access_media_buying",
                "company",
                "date_joined",
                "device_id",
                "domain",
                "email",
                "first_name",
                "google_account_id",
                "has_aw_accounts",
                "has_disapproved_ad",
                "id",
                "is_staff",
                "last_login",
                "last_name",
                "logo_url",
                "perms",
                "phone_number",
                "phone_number_verified",
                "profile_image_url",
                "role_id",
                "token",
                "is_active",
                "has_accepted_GDPR",
                "user_type",
            }
        )

    def test_set_user_verified(self):
        user = self.create_test_user()
        self.assertEqual(user.phone_number_verified, False)
        data = {
            "phone_number": "+1123456789"
        }
        with patch("userprofile.api.serializers.user.boto3.client"):
            response = self._update(data)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["phone_number_verified"], True)

    def test_handle_invalid_phone_number(self):
        self.create_test_user()
        with patch("userprofile.api.serializers.user.boto3.client", side_effect=ClientError) as mock_client:
            mock_client.admin_update_user_attributes.side_effect = ClientError
            response = self._update({
                "phone_number": "+99999999"
            })
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
