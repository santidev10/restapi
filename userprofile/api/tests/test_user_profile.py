import json

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, \
    HTTP_401_UNAUTHORIZED

from aw_reporting.models import AWConnectionToUserRelation, AWConnection
from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import Name
from userprofile.models import UserProfile
from utils.utils_tests import ExtendedAPITestCase


class UserProfileTestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.USER_PROFILE + ":" + Name.USER_PROFILE)

    def _update(self, data):
        return self.client.put(self._url, json.dumps(data),
                               content_type="application/json")

    def test_require_auth(self):
        response = self._update(dict())

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_set_default_aw_account_error_on_wrong_connection(self):
        user = self.create_test_user()
        any_user = UserProfile.objects.create()
        user_connection = AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="test_email"),
            user=any_user
        )
        self.assertIsNone(user.historical_aw_account)

        data = dict(historical_aw_account=user_connection.id)
        response = self._update(data)

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        user.refresh_from_db()
        self.assertIsNone(user.historical_aw_account)

    def test_set_default_aw_account_success_set(self):
        user = self.create_test_user()
        user_connection = AWConnectionToUserRelation.objects.create(
            connection=AWConnection.objects.create(email="test_email"),
            user=user
        )
        self.assertIsNone(user.historical_aw_account)

        data = dict(historical_aw_account=user_connection.id)
        response = self._update(data)

        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertIsNotNone(user.historical_aw_account)
