from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_creation.models import AccountCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID, DEMO_CAMPAIGNS_COUNT, DEMO_AD_GROUPS
from aw_reporting.models import Account, Campaign, AdGroup, AWConnectionToUserRelation, AWConnection
from saas.utils_tests import ExtendedAPITestCase


class AccountNamesAPITestCase(ExtendedAPITestCase):

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
            {
                'id',
                'name',
                'start_date',
                'end_date',
                'status',
                'ad_groups',
            }
        )
        self.assertEqual(len(campaign['ad_groups']), ad_groups_count)
        ad_group = campaign['ad_groups'][0]
        self.assertEqual(
            set(ad_group.keys()),
            {
                'id',
                'name',
                'status',
            }
        )

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
            {
                'id',
                'name',
                'start_date',
                'end_date',
                'status',
                'ad_groups',
            }
        )
        self.assertEqual(len(campaign['ad_groups']), len(DEMO_AD_GROUPS))
        ad_group = campaign['ad_groups'][0]
        self.assertEqual(
            set(ad_group.keys()),
            {
                'id',
                'name',
                'status',
            }
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
            {
                'id',
                'name',
                'start_date',
                'end_date',
                'status',
                'ad_groups',
            }
        )
        self.assertEqual(len(campaign['ad_groups']), len(DEMO_AD_GROUPS))
        ad_group = campaign['ad_groups'][0]
        self.assertEqual(
            set(ad_group.keys()),
            {
                'id',
                'name',
                'status',
            }
        )
