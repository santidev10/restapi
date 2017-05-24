from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from saas.utils_tests import ExtendedAPITestCase
from urllib.parse import urlencode
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
        print(response.data)
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

        response = self.client.post(
            url,
            json.dumps(dict(
                code="4/9oRTm3ncy0vqFWuaYSUCxD2cLzW8b-H4kyGKXhS4R8U#"
            )),
            content_type='application/json',
        )
        print(response.data)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("authorize_url", response.data)
