from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from urllib.parse import urlencode
from unittest.mock import patch
from aw_creation.models import AccountCreation
from aw_reporting.models import Account, AWConnectionToUserRelation, AWConnection, AWAccountPermission
from .base import AwReportingAPITestCase
import json


class AccountConnectionPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_fail_get(self):
        url = reverse("aw_reporting_urls:connect_aw_account")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_get(self):
        url = reverse("aw_reporting_urls:connect_aw_account")
        response = self.client.get(
            "{}?{}".format(
                url,
                urlencode(dict(
                    redirect_url="https://saas.channelfactory.com"
                ))
            )
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("authorize_url", response.data)

    def test_success_post(self):
        base_url = reverse("aw_reporting_urls:connect_aw_account")
        url = "{}?{}".format(
            base_url,
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
        with patch(
            "aw_reporting.api.views.client.OAuth2WebServerFlow"
        ) as flow:
            flow().step2_exchange().refresh_token = "^test_refresh_token$"
            test_email = "test@mail.kz"
            with patch(
                "aw_reporting.api.views.get_google_access_token_info",
                new=lambda _: dict(email=test_email)
            ):
                with patch("aw_reporting.api.views.get_customers",
                           new=lambda *_, **k: test_customers):
                    with patch(
                        "aw_reporting.api.views.upload_initial_aw_data"
                    ) as initial_upload_task:
                        response = self.client.post(
                            url,
                            json.dumps(dict(code="1111")),
                            content_type='application/json',
                        )
                        self.assertEqual(initial_upload_task.delay.call_count, 1)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()),
                         {'email', 'mcc_accounts', 'created', 'update_time'})
        self.assertEqual(response.data['email'], test_email)
        self.assertEqual(len(response.data['mcc_accounts']), 1,
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
            name="This item won't be deleted", account=account, owner=self.user,
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
            name="This item will be deleted", account=account_1, owner=self.user,
        )
        # the tests
        url = reverse("aw_reporting_urls:aw_account_connection", args=(connection_1.email,))
        response = self.client.delete(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['email'], connection.email)

        self.assertRaises(AccountCreation.DoesNotExist, account_creation_1.refresh_from_db)
        account_1.refresh_from_db()

        account_creation.refresh_from_db()  # this works fine



