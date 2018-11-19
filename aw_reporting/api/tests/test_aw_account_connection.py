import json
from unittest.mock import patch
from urllib.parse import urlencode

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_creation.models import AccountCreation
from aw_reporting.api.urls.names import Name
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from .base import AwReportingAPITestCase


class AccountConnectionListAPITestCase(AwReportingAPITestCase):
    _url = reverse(Name.AWAccounts.CONNECTION_LIST, [Namespace.AW_REPORTING])

    def setUp(self):
        self.user = self.create_test_user()

    def test_fail_get(self):
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_get(self):
        response = self.client.get(
            "{}?{}".format(
                self._url,
                urlencode(dict(
                    redirect_url="https://saas.channelfactory.com"
                ))
            )
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("authorize_url", response.data)

    def test_success_post(self):
        url = "{}?{}".format(
            self._url,
            urlencode(dict(
                redirect_url="https://saas.channelfactory.com"
            ))
        )
        test_customers = [
            dict(
                currencyCode="UAH",
                customerId=7046445553,
                dateTimeZone="Europe/Kiev",
                descriptiveName="MCC Account",
                companyName=None,
                canManageClients=True,
                testAccount=False,
            ),
            dict(
                customerId=7046445552,
                currencyCode="UAH",
                dateTimeZone="Europe/Kiev",
                descriptiveName="Account",
                companyName=None,
                canManageClients=False,  # !!
                testAccount=False,
            ),
        ]
        view_path = "aw_reporting.api.views.connect_aw_account"
        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
                patch(view_path + ".get_google_access_token_info",
                      new=lambda _: dict(email=test_email)), \
                patch(view_path + ".get_customers",
                      new=lambda *_, **k: test_customers), \
                patch(view_path +
                      ".upload_initial_aw_data") as initial_upload_task:
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            test_email = "test@mail.kz"
            response = self.client.post(
                url,
                json.dumps(dict(code="1111")),
                content_type="application/json",
            )
            self.assertEqual(initial_upload_task.delay.call_count, 1)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()),
                         {"id", "email", "mcc_accounts", "created",
                          "update_time"})
        self.assertEqual(response.data["email"], test_email)
        self.assertEqual(len(response.data["mcc_accounts"]), 1,
                         "MCC account is created and linked to the user")

        accounts = Account.objects.filter(
            mcc_permissions__aw_connection__user_relations__user=self.user)
        self.assertEqual(len(accounts), 1,
                         "MCC account is created and linked to the user")
        self.assertEqual(accounts[0].name, "MCC Account")

    def test_creates_account_creation(self):
        url = "{}?{}".format(
            self._url,
            urlencode(dict(
                redirect_url="https://saas.channelfactory.com"
            ))
        )
        self.assertFalse(Account.objects.all().exists())
        self.assertFalse(AccountCreation.objects.all().exists())
        account_id = next(int_iterator)
        test_customers = [
            dict(
                currencyCode="UAH",
                customerId=account_id,
                dateTimeZone="Europe/Kiev",
                descriptiveName="MCC Account",
                companyName=None,
                canManageClients=True,
                testAccount=False,
            ),
        ]
        test_email = "test@mail.com"
        view_path = "aw_reporting.api.views.connect_aw_account"
        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
                patch(view_path + ".get_google_access_token_info", new=lambda _: dict(email=test_email)), \
                patch(view_path + ".get_customers", new=lambda *_, **k: test_customers), \
                patch(view_path + ".upload_initial_aw_data") as initial_upload_task:
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            response = self.client.post(url, dict(code="1111"))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(Account.objects.filter(id=account_id).exists())
        self.assertTrue(AccountCreation.objects.filter(account_id=account_id).exists())

    def test_grant_user_permissions(self):
        user = self.user
        Permissions.sync_groups()
        self.assertFalse(self.user.has_custom_user_group(PermissionGroupNames.SELF_SERVICE))
        self.assertFalse(self.user.has_custom_user_group(PermissionGroupNames.SELF_SERVICE_TRENDS))
        url = "{}?{}".format(
            self._url,
            urlencode(dict(
                redirect_url="https://saas.channelfactory.com"
            ))
        )
        account_id = next(int_iterator)
        test_email = "test@mail.com"
        view_path = "aw_reporting.api.views.connect_aw_account"
        test_customers = [
            dict(
                currencyCode="UAH",
                customerId=account_id,
                dateTimeZone="Europe/Kiev",
                descriptiveName="MCC Account",
                companyName=None,
                canManageClients=True,
                testAccount=False,
            ),
        ]
        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
                patch(view_path + ".get_google_access_token_info", new=lambda _: dict(email=test_email)), \
                patch(view_path + ".get_customers", new=lambda *_, **k: test_customers), \
                patch(view_path + ".upload_initial_aw_data") as initial_upload_task:
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            response = self.client.post(url, dict(code="1111"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertTrue(user.has_custom_user_group(PermissionGroupNames.SELF_SERVICE))
        self.assertTrue(user.has_custom_user_group(PermissionGroupNames.SELF_SERVICE_TRENDS))


class AccountConnectionAPITestCase(AwReportingAPITestCase):
    def _get_url(self, connection_email):
        return reverse(Name.AWAccounts.CONNECTION, [Namespace.AW_REPORTING], args=(connection_email,))

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_delete(self):
        # first item
        connection = AWConnection.objects.create(
            email="me@mail.kz",
            refresh_token="",
        )
        AWConnectionToUserRelation.objects.create(
            connection=connection,
            user=self.user,
        )
        account = Account.objects.create(id="1", name="")
        manager = Account.objects.create(id="2", name="")
        account.managers.add(manager)
        AWAccountPermission.objects.create(
            aw_connection=connection,
            account=manager,
        )
        # second item
        connection_1 = AWConnection.objects.create(
            email="you@mail.kz",
            refresh_token="",
        )
        connection_to_user = AWConnectionToUserRelation.objects.create(
            connection=connection_1,
            user=self.user,
        )
        account_1 = Account.objects.create(id="3", name="")
        manager_1 = Account.objects.create(id="4", name="")
        account_1.managers.add(manager_1)
        AWAccountPermission.objects.create(
            aw_connection=connection_1,
            account=manager_1,
        )
        # the tests
        url = self._get_url(connection_1.email)
        response = self.client.delete(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['email'], connection.email)

        connection_1.refresh_from_db()
        self.assertRaises(AWConnectionToUserRelation.DoesNotExist,
                          connection_to_user.refresh_from_db)

    def test_leaves_account_creation_on_unlink(self):
        user = self.user
        manager = Account.objects.create(id=next(int_iterator))
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account.managers.add(manager)
        connection = AWConnection.objects.create(
            email="you@mail.kz",
            refresh_token="",
        )
        AWAccountPermission.objects.create(aw_connection=connection, account=manager)
        AWConnectionToUserRelation.objects.create(user=user, connection=connection)
        account_creation = AccountCreation.objects.create(owner=user, account=account)

        url = self._get_url(connection.email)
        response = self.client.delete(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertTrue(AccountCreation.objects.filter(id=account_creation.id).exists())
