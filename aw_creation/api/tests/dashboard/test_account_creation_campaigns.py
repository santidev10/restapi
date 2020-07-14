from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions
from utils.demo.recreate_test_demo_data import recreate_test_demo_data
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class DashboardAccountCreationCampaignsAPITestCase(ExtendedAPITestCase):
    @classmethod
    def setUpClass(cls):
        super(DashboardAccountCreationCampaignsAPITestCase, cls).setUpClass()
        Permissions.sync_groups()

    def _get_url(self, account_id):
        return reverse(Name.Dashboard.CAMPAIGNS, [RootNamespace.AW_CREATION, Namespace.DASHBOARD],
                       args=(account_id,))

    def test_success_get_chf_account(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="")
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id]
        }
        user.add_custom_user_group(PermissionGroupNames.MANAGED_SERVICE)
        campaign_id = 1
        ad_group_id = 1
        campaign = Campaign.objects.create(
            id=campaign_id, name="", account=account)
        AdGroup.objects.create(id=ad_group_id, name="", campaign=campaign)
        url = self._get_url(account.account_creation.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign = response.data[0]
        self.assertEqual(campaign["id"], campaign_id)
        self.assertEqual(campaign["ad_groups"][0]["id"], ad_group_id)

    def test_demo_account_campaigns_status_not_none(self):
        user = self.create_test_user()
        user.add_custom_user_group(PermissionGroupNames.MANAGED_SERVICE)
        recreate_test_demo_data()
        account = Account.objects.get(id=DEMO_ACCOUNT_ID)
        url = self._get_url(account.account_creation.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertGreater(len(response.data), 0)
        self.assertNotIn(None, [c["status"] for c in response.data])
