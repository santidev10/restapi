from django.urls import reverse
from django.http import QueryDict
from django.test import override_settings
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase


class VisibleAccountsTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.Admin.VISIBLE_ACCOUNTS)

    def test_success(self):
        user = self.create_admin_user()
        query_params = QueryDict(mutable=True)
        query_params.update(user_id=user.id)
        url = "?".join([self.url, query_params.urlencode()])

        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_demo_account_is_first(self):
        chf_manager = Account.objects.create(id="manager")
        account = Account.objects.create(id="123", name="123")
        account.managers.add(chf_manager)

        recreate_demo_data()
        user = self.create_admin_user()
        query_params = QueryDict(mutable=True)
        query_params.update(user_id=user.id)
        url = "?".join([self.url, query_params.urlencode()])

        with override_settings(CHANNEL_FACTORY_ACCOUNT_ID=chf_manager.id):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        ids = [i["id"] for i in response.data]
        self.assertEqual(ids, [DEMO_ACCOUNT_ID, account.id])
