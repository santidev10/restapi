from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from saas.utils_tests import ExtendedAPITestCase
from urllib.parse import urlencode
from unittest.mock import patch
from aw_reporting.models import Account
import json


class AccountConnectionPITestCase(ExtendedAPITestCase):

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
                         {'email', 'mcc_accounts'})
        self.assertEqual(response.data['email'], test_email)
        self.assertEqual(len(response.data['mcc_accounts']), 1,
                         "MCC account is created and linked to the user")

        accounts = Account.objects.filter(
            mcc_permissions__aw_connection__users=self.user)
        self.assertEqual(len(accounts), 1,
                         "MCC account is created and linked to the user")
        self.assertEqual(accounts[0].name, "MCC Account")

