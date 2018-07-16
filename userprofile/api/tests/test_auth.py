import json

from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from django.db import IntegrityError
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.tests.base import AwReportingAPITestCase, Account, \
    AWConnection, AWAccountPermission, \
    AWConnectionToUserRelation, Campaign, Ad, AdGroup
from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import Name


class AuthAPITestCase(AwReportingAPITestCase):
    _url = reverse(Namespace.USER_PROFILE + ":" + Name.AUTH)

    def test_success(self):
        user = self.create_test_user()
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data),
            {
                "id", "profile_image_url", "company", "phone_number",
                "is_staff", "last_name", "has_aw_accounts",
                "date_joined", "last_login", "email", "first_name", "token",
                "can_access_media_buying", "has_disapproved_ad", "vendor",
                "access", "aw_settings", "historical_aw_account",
                "google_account_id"
            }
        )

    def test_success_has_connected_accounts(self):
        user = self.create_test_user()
        self.create_account(user)
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["has_aw_accounts"], True)

    def test_success_has_no_connected_accounts(self):
        user = self.create_test_user()
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["has_aw_accounts"], False)

    def test_success_has_no_disapproved_ad(self):
        user = self.create_test_user()
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["has_disapproved_ad"], False)

    def test_success_has_disapproved_ad(self):
        user = self.create_test_user()
        account = Account.objects.create(id="1", name="",
                                         can_manage_clients=True)
        connection = AWConnection.objects.create()
        AWAccountPermission.objects.create(aw_connection=connection,
                                           account=account, can_read=True)
        AWConnectionToUserRelation.objects.create(
            connection=connection,
            user=user
        )
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        Ad.objects.create(ad_group=ad_group, is_disapproved=True)
        response = self.client.post(
            self._url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["has_disapproved_ad"], True)

    def test_case_insensitive(self):
        email = "MixedCase@Email.com"
        password = "test_password"
        user = get_user_model().objects.create(
            email=email
        )
        user.set_password(password)
        user.save()

        payload_exact = dict(username=email, password=password)
        payload_upper = dict(username=email.upper(), password=password)
        payload_lower = dict(username=email.lower(), password=password)

        for payload in (payload_exact, payload_upper, payload_lower):
            response = self.client.post(self._url,
                                        json.dumps(payload),
                                        content_type="application/json")

            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_user_email_should_be_stored_in_lowercase(self):
        test_email = "Test@email.com"
        test_email_lower = test_email.lower()
        self.assertNotEqual(test_email, test_email.lower())

        user = get_user_model().objects.create(email=test_email)

        stored_in_lowercase = get_user_model().objects \
            .filter(id=user.id, email=test_email_lower) \
            .exists()
        self.assertTrue(stored_in_lowercase)

        user.refresh_from_db()
        self.assertEqual(user.email, test_email_lower)

    def test_user_unique_by_email_case_insensitive(self):
        test_email = "Test@email.com"

        def create_user(email):
            return get_user_model().objects.create(email=email)

        create_user(test_email)
        try:
            create_user(test_email.upper())
        except IntegrityError:
            pass
        else:
            self.fail()
