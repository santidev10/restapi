from django.http import QueryDict
from django.test import override_settings
from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace
from userprofile.constants import UserSettingsKey
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


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
        chf_manager = Account.objects.create(id=111)
        account = Account.objects.create(id=123, name="123")
        account.managers.add(chf_manager)

        recreate_demo_data()
        user = self.create_admin_user()
        query_params = QueryDict(mutable=True)
        query_params.update(user_id=user.id)
        url = "?".join([self.url, query_params.urlencode()])

        with override_settings(MCC_ACCOUNT_IDS=[chf_manager.id]):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        ids = [i["id"] for i in response.data]
        self.assertEqual(ids, [DEMO_ACCOUNT_ID, account.id])

    def test_visibility(self):
        chf_manager = Account.objects.create(id=111)
        account_1 = Account.objects.create(id=next(int_iterator))
        account_1.managers.add(chf_manager)
        account_2 = Account.objects.create(id=next(int_iterator))
        account_2.managers.add(chf_manager)
        # user = self.create_test_user()
        user = self.create_admin_user()

        user.aw_settings[UserSettingsKey.VISIBLE_ACCOUNTS] = [account_1.id]
        user.save()

        query_params = QueryDict(mutable=True)
        query_params.update(user_id=user.id)
        url = "?".join([self.url, query_params.urlencode()])
        with override_settings(MCC_ACCOUNT_IDS=[chf_manager.id]):
            response = self.client.get(url)

        visible_by_id = {acc["id"]: acc["visible"] for acc in response.data}
        self.assertTrue(visible_by_id[account_1.id])
        self.assertFalse(visible_by_id[account_2.id])
