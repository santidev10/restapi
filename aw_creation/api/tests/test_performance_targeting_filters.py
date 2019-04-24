from datetime import datetime

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.models import AccountCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic, AWConnectionToUserRelation, AWConnection
from utils.utittests.test_case import ExtendedAPITestCase


class AccountNamesAPITestCase(ExtendedAPITestCase):
    def test_success_get_is_managed_false(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user, account=account, is_managed=False,
                                                          is_approved=True)
        start = datetime(2009, 3, 10).date()
        end = datetime(2017, 1, 1).date()

        campaign = Campaign.objects.create(id=1, name="C2342", account=account,
                                           start_date=start, end_date=end, status="Hmm...")
        ad_group = AdGroup.objects.create(id=1, name="A5454", campaign=campaign)

        stats = dict(
            ad_group=ad_group,
            impressions=10,
            video_views=5,
            clicks=1,
            cost=1,
            average_position=1
        )
        AdGroupStatistic.objects.create(date=start, **stats)
        AdGroupStatistic.objects.create(date=end, **stats)

        url = reverse("aw_creation_urls:performance_targeting_filters",
                      args=(account_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'start_date', 'end_date', 'campaigns',
                'targeting', 'group_by',
            }
        )
        self.assertEqual(response.data['start_date'], start)
        self.assertEqual(response.data['end_date'], end)
        self.assertEqual(len(response.data['campaigns']), 1)
        campaign_data = response.data['campaigns'][0]
        self.assertEqual(campaign_data['name'], campaign.name)
        self.assertEqual(campaign_data['start_date'], campaign.start_date)
        self.assertEqual(campaign_data['end_date'], campaign.end_date)
        self.assertEqual(campaign_data['status'], campaign.status)

    def test_success_no_ad_groups(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user, account=account, is_managed=False,
                                                          is_approved=True)
        Campaign.objects.create(id=1, name="", account=account)

        url = reverse("aw_creation_urls:performance_targeting_filters",
                      args=(account_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['campaigns']), 1)

    def test_success_get_demo(self):
        recreate_demo_data()
        self.create_test_user()
        url = reverse("aw_creation_urls:performance_targeting_filters",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'start_date', 'end_date', 'campaigns',
                'targeting', 'group_by',
            }
        )
        self.assertEqual(len(response.data['campaigns']), 2)
        account = Account.objects.get(pk=DEMO_ACCOUNT_ID)
