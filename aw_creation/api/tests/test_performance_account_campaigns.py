from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_creation.models import AccountCreation, CampaignCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID, DEMO_CAMPAIGNS_COUNT, DEMO_AD_GROUPS
from aw_reporting.models import Account, Campaign, AdGroup, AWConnectionToUserRelation, AWConnection
from saas.utils_tests import ExtendedAPITestCase
from django.utils import timezone


class AccountNamesAPITestCase(ExtendedAPITestCase):
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

    def test_success_get(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user, account=account, is_managed=False)

        campaigns_count = 3
        ad_groups_count = 2
        for i in range(campaigns_count):
            c = Campaign.objects.create(id=i, name="", account=account)

            for j in range(ad_groups_count):
                AdGroup.objects.create(id="{}{}".format(i, j), name="", campaign=c)

        url = reverse("aw_creation_urls:performance_account_campaigns",
                      args=(account_creation.id,))

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
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user, account=account,
                                                          is_managed=True, sync_at=timezone.now())
        campaign_creation = CampaignCreation.objects.create(name="WW", account_creation=account_creation)

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

        url = reverse("aw_creation_urls:performance_account_campaigns",
                      args=(account_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        for campaign in response.data:
            campaign_creation_id = None
            if campaign["id"] == managed_campaign.id:
                campaign_creation_id = campaign_creation.id
            self.assertIs(campaign['campaign_creation_id'], campaign_creation_id)

    def test_success_get_demo(self):
        self.create_test_user()
        url = reverse("aw_creation_urls:performance_account_campaigns",
                      args=(DEMO_ACCOUNT_ID,))
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
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user)

        url = reverse("aw_creation_urls:performance_account_campaigns",
                      args=(account_creation.id,))

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
