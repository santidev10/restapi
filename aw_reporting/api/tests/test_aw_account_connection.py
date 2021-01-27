import json
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch
from urllib.parse import urlencode

from django.http import QueryDict
from googleads.errors import AdWordsReportBadRequestError
from requests import HTTPError
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_creation.models import AccountCreation
from aw_reporting.adwords_reports import AWErrorType
from aw_reporting.api.urls.names import Name
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
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
                  ".upload_initial_aw_data_task") as initial_upload_task:
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
            patch(view_path + ".upload_initial_aw_data_task"):
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            response = self.client.post(url, dict(code="1111"))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(Account.objects.filter(id=account_id).exists())
        self.assertTrue(AccountCreation.objects.filter(account_id=account_id).exists())

    def test_grant_user_permissions(self):
        user = self.user
        self.assertFalse(user.has_permission(StaticPermissions.MANAGED_SERVICE))
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
            patch(view_path + ".upload_initial_aw_data_task"):
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            response = self.client.post(url, dict(code="1111"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertTrue(user.has_permission(StaticPermissions.MANAGED_SERVICE))

    def test_handle_inactive_account(self):
        tz = "UTC"
        mcc_account = Account.objects.create(id=next(int_iterator), timezone=tz,
                                             can_manage_clients=True)
        AWAccountPermission.objects.create(account=mcc_account,
                                           aw_connection=AWConnection.objects.create(),
                                           can_read=True)

        account = Account.objects.create(id=next(int_iterator), timezone=tz)
        account.managers.add(mcc_account)
        account.save()
        test_customers = [
            dict(
                currencyCode="UAH",
                customerId=mcc_account.id,
                dateTimeZone=tz,
                descriptiveName="MCC Account",
                companyName=mcc_account.name,
                canManageClients=True,
                testAccount=False,
            ),
        ]

        query_params = QueryDict("", mutable=True)
        query_params.update(redirect_url="https://saas.channelfactory.com")
        url = "?".join([self._url, query_params.urlencode()])
        exception = AdWordsReportBadRequestError(
            AWErrorType.NOT_ACTIVE,
            "<null>",
            None,
            HTTP_400_BAD_REQUEST,
            HTTPError(),
            "XML Body"
        )
        test_email = "test@email.com"
        aw_client_mock = MagicMock()
        downloader_mock = aw_client_mock.GetReportDownloader().DownloadReportAsStream
        downloader_mock.side_effect = exception
        view_path = "aw_reporting.api.views.connect_aw_account"
        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
            patch(view_path + ".get_customers", new=lambda *_, **k: test_customers), \
            patch(view_path + ".get_google_access_token_info", new=lambda _: dict(email=test_email)), \
            patch("aw_reporting.adwords_api.adwords.AdWordsClient", return_value=aw_client_mock), \
            patch.object(GoogleAdsUpdater, "MAX_RETRIES", new_callable=PropertyMock(return_value=0)):
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            response = self.client.post(url, dict(code="1111"))

        self.assertEqual(response.status_code, HTTP_200_OK)
        account.refresh_from_db()
        self.assertFalse(account.is_active)

    def test_link_account_skip_inactive(self):
        tz = "UTC"
        mcc_account = Account.objects.create(id=next(int_iterator), timezone=tz,
                                             can_manage_clients=True)
        AWAccountPermission.objects.create(account=mcc_account,
                                           aw_connection=AWConnection.objects.create(),
                                           can_read=True)

        account = Account.objects.create(id=next(int_iterator), timezone=tz, is_active=False)
        account.managers.add(mcc_account)
        account.save()

        test_customers = [
            dict(
                currencyCode="UAH",
                customerId=mcc_account.id,
                dateTimeZone=tz,
                descriptiveName="MCC Account",
                companyName=mcc_account.name,
                canManageClients=True,
                testAccount=False,
            ),
        ]
        view_path = "aw_reporting.api.views.connect_aw_account"

        query_params = QueryDict("", mutable=True)
        query_params.update(redirect_url="https://saas.channelfactory.com")
        url = "?".join([self._url, query_params.urlencode()])
        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
            patch(view_path + ".get_customers", new=lambda *_, **k: test_customers), \
            patch(view_path + ".get_google_access_token_info", new=lambda _: dict(email="test@mail.com")), \
            patch.object(GoogleAdsUpdater, "full_update") as account_update_mock:
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            response = self.client.post(url, dict(code="1111"))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertFalse(account_update_mock.called)


class AccountConnectionDeleteAPITestCase(AwReportingAPITestCase):
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
        self.assertEqual(response.data[0]["email"], connection.email)

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
