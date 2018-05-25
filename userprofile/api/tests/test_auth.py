import json

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.tests.base import AwReportingAPITestCase, Account, AWConnection, AWAccountPermission, \
    AWConnectionToUserRelation, Campaign, Ad, AdGroup


class AuthAPITestCase(AwReportingAPITestCase):
    def test_success(self):
        user = self.create_test_user()
        url = reverse("userprofile_api_urls:user_auth")
        response = self.client.post(
            url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data),
            {
                'id', 'profile_image_url', 'company', 'phone_number', 'is_staff', 'last_name', 'has_aw_accounts',
                'date_joined', 'last_login', 'email', 'first_name', 'token',
                'can_access_media_buying', 'has_disapproved_ad', 'vendor',
                'access'
            }
        )

    def test_success_has_connected_accounts(self):
        user = self.create_test_user()
        self.create_account(user)
        url = reverse("userprofile_api_urls:user_auth")
        response = self.client.post(
            url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data['has_aw_accounts'], True)

    def test_success_has_no_connected_accounts(self):
        user = self.create_test_user()
        url = reverse("userprofile_api_urls:user_auth")
        response = self.client.post(
            url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data['has_aw_accounts'], False)

    def test_success_has_no_disapproved_ad(self):
        user = self.create_test_user()
        url = reverse("userprofile_api_urls:user_auth")
        response = self.client.post(
            url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data['has_disapproved_ad'], False)

    def test_success_has_disapproved_ad(self):
        user = self.create_test_user()
        account = Account.objects.create(id='1', name='', can_manage_clients=True)
        connection = AWConnection.objects.create()
        AWAccountPermission.objects.create(aw_connection=connection, account=account, can_read=True)
        AWConnectionToUserRelation.objects.create(
            connection=connection,
            user=user
        )
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        Ad.objects.create(ad_group=ad_group, is_disapproved=True)

        url = reverse("userprofile_api_urls:user_auth")
        response = self.client.post(
            url, json.dumps(dict(auth_token=user.auth_token.key)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data['has_disapproved_ad'], True)
