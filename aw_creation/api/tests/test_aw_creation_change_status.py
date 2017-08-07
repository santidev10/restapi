from django.core.urlresolvers import reverse
from aw_creation.models import *
from aw_reporting.models import *
from datetime import timedelta
from aw_reporting.api.tests.base import AwReportingAPITestCase


class ChangedAccountsAPITestCase(AwReportingAPITestCase):

    def test_success_get(self):
        user = self.create_test_user(auth=False)
        account = Account.objects.create(id="123", name="")
        ac_creation = AccountCreation.objects.create(
            name="", owner=user, account=account, is_approved=True
        )
        # --
        url = reverse("aw_creation_urls:aw_creation_change_status", args=(account.id,))

        self.client.patch(
            url, json.dumps({"updated_at": str(ac_creation.updated_at - timedelta(seconds=2))}),
            content_type='application/json',
        )
        ac_creation.refresh_from_db()
        self.assertEqual(ac_creation.is_changed, True)

        self.client.patch(
            url, json.dumps({"updated_at": str(ac_creation.updated_at)}), content_type='application/json',
        )
        ac_creation.refresh_from_db()
        self.assertEqual(ac_creation.is_changed, False)


