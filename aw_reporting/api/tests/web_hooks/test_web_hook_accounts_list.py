from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase


class WebHookAWAccountsListTestCase(ExtendedAPITestCase):
    def _get_url(self, manager_id):
        return reverse(
            Namespace.AW_REPORTING + ":" + Name.WebHook.ACCOUNTS_LIST,
            args=(manager_id,))

    def test_returns_only_first_level_child_accounta(self):
        manager_1 = Account.objects.create(id=1)
        manager_2 = Account.objects.create(id=2)
        account_1 = Account.objects.create(id=3)
        account_2 = Account.objects.create(id=4)

        account_1.managers.add(manager_1)
        account_2.managers.add(manager_2)
        account_1.managers.add(manager_2)
        account_1.save()
        account_2.save()

        response = self.client.get(self._get_url(manager_1.id))

        self.assertEqual(response.status_code, HTTP_200_OK)
        account_1.refresh_from_db()
        self.assertEqual(response.data, [account_1.id])
