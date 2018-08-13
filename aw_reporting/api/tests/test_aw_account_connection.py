import json
from unittest.mock import patch
from urllib.parse import urlencode

from django.core.urlresolvers import reverse
from django.test import override_settings
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_creation.models import AccountCreation
from aw_reporting.api.urls.names import Name
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import get_user_model
from saas.urls.namespaces import Namespace
from utils.utils_tests import int_iterator
from .base import AwReportingAPITestCase


class AccountConnectionPITestCase(AwReportingAPITestCase):
    _url = reverse(Namespace.AW_REPORTING + ":" + Name.AWAccounts.ACCOUNT)

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
        account_creation = AccountCreation.objects.create(
            name="This item won't be deleted", account=account,
            owner=self.user,
        )
        # second item
        connection_1 = AWConnection.objects.create(
            email="you@mail.kz",
            refresh_token="",
        )
        AWConnectionToUserRelation.objects.create(
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
        account_creation_1 = AccountCreation.objects.create(
            name="This item will be deleted", account=account_1,
            owner=self.user,
        )
        # the tests
        url_path = Namespace.AW_REPORTING + ":" + Name.AWAccounts.CONNECTION
        url = reverse(url_path, args=(connection_1.email,))
        response = self.client.delete(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['email'], connection.email)

        self.assertRaises(AccountCreation.DoesNotExist,
                          account_creation_1.refresh_from_db)
        account_1.refresh_from_db()

        account_creation.refresh_from_db()  # this works fine

    def test_does_not_remove_user_through_historical_account_relation(self):
        connection_1 = AWConnection.objects.create(
            email="you@mail.kz",
            refresh_token="",
        )
        user_connection = AWConnectionToUserRelation.objects.create(
            connection=connection_1,
            user=self.user,
        )
        self.user.historical_aw_account = user_connection
        self.user.save()

        url_path = Namespace.AW_REPORTING + ":" + Name.AWAccounts.CONNECTION
        url = reverse(url_path, args=(connection_1.email,))
        response = self.client.delete(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        user_exists = get_user_model().objects.filter(id=self.user.id).exists()
        self.assertTrue(user_exists)

    @override_settings(DISABLE_ACCOUNT_CREATION_AUTO_CREATING=False)
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
