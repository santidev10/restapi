import json
from unittest.mock import MagicMock
from unittest.mock import patch
from urllib.parse import urlencode
from types import SimpleNamespace

from django.http import QueryDict
from google.ads.google_ads.errors import GoogleAdsException
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from .base import AwReportingAPITestCase
from aw_creation.models import AccountCreation
from aw_reporting.api.urls.names import Name
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from aw_reporting.google_ads.tasks.upload_initial_aw_data import upload_initial_aw_data_task
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from saas import celery_app
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions
from utils.utittests.int_iterator import int_iterator
from utils.utittests.mock_google_ads_response import MockGoogleAdsAPIResponse
from utils.utittests.reverse import reverse


class AccountConnectionListAPITestCase(AwReportingAPITestCase):
    _url = reverse(Name.AWAccounts.CONNECTION_LIST, [Namespace.AW_REPORTING])

    def setUp(self):
        self.user = self.create_test_user()
        celery_app.conf.update(CELERY_ALWAYS_EAGER=True)

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
        mock_id = 7046445553
        mock_customer_client_data = MockGoogleAdsAPIResponse()
        mock_customer_client_data.set("customer_client", "id", mock_id)
        mock_customer_client_data.set("customer_client", "descriptive_name", "MCC Account")
        mock_customer_client_data.set("customer_client", "currency_code", "UAH")
        mock_customer_client_data.set("customer_client", "time_zone", "Europe/Kiev")
        mock_customer_client_data.set("customer_client", "manager", True)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        mock_customer_client_data.set("customer_client", "id", mock_id + 1)
        mock_customer_client_data.set("customer_client", "descriptive_name", "Account")
        mock_customer_client_data.set("customer_client", "currency_code", "UAH")
        mock_customer_client_data.set("customer_client", "time_zone", "Europe/Kiev")
        mock_customer_client_data.set("customer_client", "manager", False)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        view_path = "aw_reporting.api.views.connect_aw_account"
        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
                patch(view_path + ".get_google_access_token_info",
                      new=lambda _: dict(email=test_email)), \
                patch(view_path + ".AccountUpdater.get_accessible_customers",
                      new=lambda *_, **k: mock_customer_client_data), \
                patch(view_path + ".get_client", return_value=MagicMock()), \
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

        mock_customer_client_data = MockGoogleAdsAPIResponse()
        mock_customer_client_data.set("customer_client", "id", account_id)
        mock_customer_client_data.set("customer_client", "descriptive_name", "MCC Account")
        mock_customer_client_data.set("customer_client", "currency_code", "UAH")
        mock_customer_client_data.set("customer_client", "time_zone", "Europe/Kiev")
        mock_customer_client_data.set("customer_client", "manager", True)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        test_email = "test@mail.com"
        view_path = "aw_reporting.api.views.connect_aw_account"
        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
                patch(view_path + ".get_google_access_token_info", new=lambda _: dict(email=test_email)), \
                patch(view_path + ".get_client", return_value=MagicMock()), \
                patch(view_path + ".AccountUpdater.get_accessible_customers", new=lambda *_, **k: mock_customer_client_data), \
                patch(view_path + ".upload_initial_aw_data_task") as initial_upload_task:
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

        mock_customer_client_data = MockGoogleAdsAPIResponse()
        mock_customer_client_data.set("customer_client", "id", account_id)
        mock_customer_client_data.set("customer_client", "descriptive_name", "MCC Account")
        mock_customer_client_data.set("customer_client", "currency_code", "UAH")
        mock_customer_client_data.set("customer_client", "time_zone", "Europe/Kiev")
        mock_customer_client_data.set("customer_client", "manager", True)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
                patch(view_path + ".get_google_access_token_info", new=lambda _: dict(email=test_email)), \
                patch(view_path + ".AccountUpdater.get_accessible_customers", new=lambda *_, **k: mock_customer_client_data), \
                patch(view_path + ".get_client", return_value=MagicMock()), \
                patch(view_path + ".upload_initial_aw_data_task") as initial_upload_task:
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            response = self.client.post(url, dict(code="1111"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertTrue(user.has_custom_user_group(PermissionGroupNames.SELF_SERVICE))
        self.assertTrue(user.has_custom_user_group(PermissionGroupNames.SELF_SERVICE_TRENDS))

    @patch("aw_reporting.api.views.connect_aw_account.get_client", new=MagicMock())
    @patch("aw_reporting.google_ads.tasks.upload_initial_aw_data.get_client", new=MagicMock())
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

        mock_customer_client_data = MockGoogleAdsAPIResponse()
        mock_customer_client_data.set("customer_client", "id", mcc_account.id)
        mock_customer_client_data.set("customer_client", "descriptive_name", "MCC Account")
        mock_customer_client_data.set("customer_client", "currency_code", "UAH")
        mock_customer_client_data.set("customer_client", "time_zone", tz)
        mock_customer_client_data.set("customer_client", "manager", True)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        ga_exception = GoogleAdsException(None, None, MagicMock(), None)
        err = SimpleNamespace(error_code=SimpleNamespace())
        err.error_code.authorization_error = 24  # AuthorizationErrorEnum: CUSTOMER_NOT_ENABLED
        ga_exception.failure.errors = [err]

        query_params = QueryDict("", mutable=True)
        query_params.update(redirect_url="https://saas.channelfactory.com")
        url = "?".join([self._url, query_params.urlencode()])

        test_email = "test@email.com"
        view_path = "aw_reporting.api.views.connect_aw_account"
        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
                patch(view_path + ".get_google_access_token_info", new=lambda _: dict(email=test_email)), \
                patch(view_path + ".AccountUpdater.get_accessible_customers", new=lambda *_, **k: mock_customer_client_data), \
                patch(view_path + ".upload_initial_aw_data_task", new=MagicMock()), \
                patch("aw_reporting.google_ads.tasks.upload_initial_aw_data.get_client", new=MagicMock()), \
                patch("aw_reporting.google_ads.google_ads_updater.GoogleAdsUpdater.update_accounts_as_mcc", new=MagicMock()), \
                patch("aw_reporting.google_ads.updaters.campaigns.CampaignUpdater.update") as mock_update:
            # Patch CampaignUpdater since it is the first updater class used
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            mock_update.side_effect = ga_exception
            response = self.client.post(url, dict(code="1111"))
            # Manually invoke since normally ran as an async celery task in view
            upload_initial_aw_data_task(test_email)
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

        mock_customer_client_data = MockGoogleAdsAPIResponse()
        mock_customer_client_data.set("customer_client", "id", mcc_account.id)
        mock_customer_client_data.set("customer_client", "descriptive_name", "MCC Account")
        mock_customer_client_data.set("customer_client", "currency_code", "UAH")
        mock_customer_client_data.set("customer_client", "time_zone", tz)
        mock_customer_client_data.set("customer_client", "manager", True)
        mock_customer_client_data.set("customer_client", "test_account", False)
        mock_customer_client_data.add_row()

        view_path = "aw_reporting.api.views.connect_aw_account"
        query_params = QueryDict("", mutable=True)
        query_params.update(redirect_url="https://saas.channelfactory.com")
        url = "?".join([self._url, query_params.urlencode()])
        with patch(view_path + ".client.OAuth2WebServerFlow") as flow, \
                patch(view_path + ".get_client", return_value=MagicMock()), \
                patch(view_path + ".get_google_access_token_info", new=lambda _: dict(email="test@mail.com")), \
                patch(view_path + ".AccountUpdater.get_accessible_customers", return_value=mock_customer_client_data), \
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
