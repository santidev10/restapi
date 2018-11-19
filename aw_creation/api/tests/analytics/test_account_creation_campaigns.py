from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_creation.models import CampaignCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.demo.models import DEMO_AD_GROUPS
from aw_reporting.demo.models import DEMO_CAMPAIGNS_COUNT
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import campaign_type_str
from aw_reporting.settings import AdwordsAccountSettings
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse


class AnalyticsAccountCreationCampaignsAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_id):
        return reverse(
            Name.Analytics.CAMPAIGNS, [RootNamespace.AW_CREATION, Namespace.ANALYTICS],
            args=(account_id,)
        )

    campaign_keys = {
        'id',
        'name',
        'start_date',
        'end_date',
        'status',
        'ad_groups',
        'campaign_creation_id',
    }

    ad_group_keys = {
        'id',
        'name',
        'status',
    }

    def create_test_user(self, auth=True, connected=True):
        user = super(AnalyticsAccountCreationCampaignsAPITestCase, self).create_test_user(auth)
        if connected:
            AWConnectionToUserRelation.objects.create(
                # user must have a connected account not to see demo data
                connection=AWConnection.objects.create(email="me@mail.kz",
                                                       refresh_token=""),
                user=user,
            )
        return user

    def test_success_get(self):
        user = self.create_test_user()
        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          account=account,
                                                          is_managed=False,
                                                          is_approved=True)

        campaigns_count = 3
        ad_groups_count = 2
        for i in range(campaigns_count):
            c = Campaign.objects.create(id=i, name="", account=account)

            for j in range(ad_groups_count):
                AdGroup.objects.create(id="{}{}".format(i, j), name="",
                                       campaign=c)

        url = self._get_url(account_creation.id)

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), campaigns_count)
        campaign = response.data[0]
        self.assertEqual(
            set(campaign.keys()),
            self.campaign_keys,
        )
        self.assertIs(campaign['campaign_creation_id'], None)
        self.assertEqual(len(campaign['ad_groups']), ad_groups_count)
        ad_group = campaign['ad_groups'][0]
        self.assertEqual(
            set(ad_group.keys()),
            self.ad_group_keys,
        )

    def test_success_get_managed_campaign(self):
        user = self.create_test_user()
        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          account=account,
                                                          is_managed=True,
                                                          sync_at=timezone.now())
        campaign_creation = CampaignCreation.objects.create(name="WW",
                                                            account_creation=account_creation)

        managed_campaign = Campaign.objects.create(
            id="444",
            name="{} #{}".format(campaign_creation.name, campaign_creation.id),
            account=account,
        )
        AdGroup.objects.create(id="666", name="", campaign=managed_campaign)

        campaign_2 = Campaign.objects.create(
            id="554",
            name="Another campaign #code",
            account=account,
        )
        AdGroup.objects.create(id="777", name="", campaign=campaign_2)

        url = self._get_url(account_creation.id)

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        for campaign in response.data:
            campaign_creation_id = None
            if campaign["id"] == managed_campaign.id:
                campaign_creation_id = campaign_creation.id
            self.assertEqual(campaign['campaign_creation_id'],
                             campaign_creation_id)

    def test_success_get_demo(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), DEMO_CAMPAIGNS_COUNT)
        campaign = response.data[0]
        self.assertEqual(
            set(campaign.keys()),
            self.campaign_keys,
        )
        self.assertEqual(len(campaign['ad_groups']), len(DEMO_AD_GROUPS))
        ad_group = campaign['ad_groups'][0]
        self.assertEqual(
            set(ad_group.keys()),
            self.ad_group_keys,
        )

    def test_success_get_demo_data(self):
        """
        SAAS-793
        :return:
        """
        user = self.create_test_user(connected=False)

        url = self._get_url("demo")

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), DEMO_CAMPAIGNS_COUNT)
        campaign = response.data[0]
        self.assertEqual(
            set(campaign.keys()),
            self.campaign_keys,
        )
        self.assertEqual(len(campaign['ad_groups']), len(DEMO_AD_GROUPS))
        ad_group = campaign['ad_groups'][0]
        self.assertEqual(
            set(ad_group.keys()),
            self.ad_group_keys,
        )

    def test_excluded_campaings_filter_ingores(self):
        user = self.create_test_user()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            id=next(int_iterator), name="", owner=user, account=account, is_managed=True, sync_at=timezone.now())
        all_types = AdwordsAccountSettings.CAMPAIGN_TYPES
        for index, campaign_type in enumerate(all_types):
            Campaign.objects.create(id=index, type=campaign_type_str(campaign_type), account=account)
        hidden_types = all_types[::2]
        expected_types = set(all_types)
        expected_types_str = set(campaign_type_str(t) for t in expected_types)
        url = self._get_url(account_creation.id)
        user_settings = {
            UserSettingsKey.HIDDEN_CAMPAIGN_TYPES: {
                account.id: hidden_types
            }
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        ids = [c["id"] for c in response.data]
        types = Campaign.objects.filter(id__in=ids).values_list("type", flat=True)
        self.assertEqual(len(types), len(expected_types))
        self.assertEqual(set(types), expected_types_str)

    def test_campaign_without_type_are_visible(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1,
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            id=2,
            name="", owner=user, account=account, is_managed=True,
            sync_at=timezone.now())
        all_types = AdwordsAccountSettings.CAMPAIGN_TYPES

        campaign = Campaign.objects.create(id=1,
                                           type=None,
                                           account=account)
        campaign.refresh_from_db()

        url = self._get_url(account_creation.id)

        user_settings = {
            UserSettingsKey.HIDDEN_CAMPAIGN_TYPES: {
                account.id: all_types
            }
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], campaign.id)

    def test_ignores_visible_accounts_setting(self):
        user = self.create_test_user()
        account = Account.objects.create()
        account_creation = account.account_creation
        account_creation.owner = user
        account_creation.save()

        campaign = Campaign.objects.create(id=str(next(int_iterator)), account=account)
        ad_group = AdGroup.objects.create(id=str(next(int_iterator)), campaign=campaign)

        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,

        }
        url = self._get_url(account_creation.id)
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        self.assertEqual(campaign_data["id"], campaign.id)
        self.assertEqual(len(campaign_data["ad_groups"]), 1)
        ad_group_data = campaign_data["ad_groups"][0]
        self.assertEqual(ad_group_data["id"], ad_group.id)
