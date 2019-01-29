import json

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED

from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from utils.utittests.test_case import ExtendedAPITestCase


class UserProfileTestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.USER_PROFILE + ":" + UserprofilePathName.USER_PROFILE)

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
                "access",
                "aw_settings",
                "can_access_media_buying",
                "company",
                "date_joined",
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
                "phone_number",
                "profile_image_url",
                "token",
                "is_active",
                "has_accepted_GDPR",
            }
        )
