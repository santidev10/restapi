from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.models import Account, Campaign, AdGroup
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase


class DashboardAccountCreationCampaignsAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_id):
        return reverse(RootNamespace.AW_CREATION + ":" + Namespace.DASHBOARD + ":" + Name.Dashboard.CAMPAIGNS,
                       args=(account_id,))

    def test_success_get_chf_account(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="")
        user.aw_settings[UserSettingsKey.VISIBLE_ACCOUNTS] = [account.id]
        user.aw_settings[UserSettingsKey.HIDDEN_CAMPAIGN_TYPES] = {
            "".format(account.id): []
        }
        user.update_access([{"name": "Tools", "value": True}])
        user.save()
        account_creation = AccountCreation.objects.create(
            name="", account=account, owner=user)
        campaign_id = "1"
        ad_group_id = "1"
        campaign = Campaign.objects.create(
            id=campaign_id, name="", account=account)
        AdGroup.objects.create(id=ad_group_id, name="", campaign=campaign)
        url = self._get_url(account_creation.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign = response.data[0]
        self.assertEqual(campaign["id"], campaign_id)
        self.assertEqual(campaign["ad_groups"][0]["id"], ad_group_id)
