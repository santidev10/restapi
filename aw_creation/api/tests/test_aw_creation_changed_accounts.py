from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from django.utils import timezone
from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from aw_reporting.api.tests.base import AwReportingAPITestCase


class ChangedAccountsAPITestCase(AwReportingAPITestCase):

    def test_success_get(self):
        user = self.create_test_user(auth=False)
        manager = Account.objects.create(id=1, name="")
        account = Account.objects.create(id="123", name="")
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

