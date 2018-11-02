from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.models import AccountCreation
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.models import Account


class ChangedAccountsAPITestCase(AwReportingAPITestCase):

    def test_success_get(self):
        user = self.create_test_user(auth=False)
        manager = Account.objects.create(id=1, name="")
        account = Account.objects.create(id="123", name="",
                                         skip_creating_account_creation=True)
        account.managers.add(manager)
        AccountCreation.objects.create(
            name="", owner=user, account=account, is_approved=True
        )
        # --
        url = reverse("aw_creation_urls:aw_creation_changed_accounts_list", args=(manager.id,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        self.assertIn(account.id, data)
