from datetime import datetime, timedelta
from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_202_ACCEPTED
from urllib.parse import urlencode
from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from aw_reporting.api.tests.base import AwReportingAPITestCase


class AccountListAPITestCase(AwReportingAPITestCase):

    details_keys = {
        'id', 'name', 'status', 'start', 'end', 'is_managed',
        'is_optimization_active', 'is_changed', 'weekly_chart',
        'video_views', 'cost', 'video_view_rate', 'ctr_v', 'impressions', 'clicks',
    }

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_post(self):
        for uid, name in ((1000, "English"), (1003, "Spanish")):
            Language.objects.get_or_create(id=uid, name=name)

        url = reverse("aw_creation_urls:account_creation_list")
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)

        self.assertEqual(
            set(response.data.keys()),
            {
                'id', 'name', 'campaign_creations',
                'updated_at', 'is_ended', 'is_paused', 'is_approved',
            }
        )

        campaign_creation = response.data['campaign_creations'][0]
        self.assertEqual(
            set(campaign_creation.keys()),
            {
                'id', 'name', 'updated_at',
                'parents', 'content_exclusions', 'genders', 'age_ranges',
                'start', 'end', 'budget', 'languages', 'devices',
                'frequency_capping', 'ad_schedule_rules',
                'location_rules', 'ad_group_creations',
                'video_networks', 'video_ad_format', 'delivery_method',
            }
        )
        self.assertEqual(len(campaign_creation['languages']), 2)

        ad_group_creation = campaign_creation['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_creation.keys()),
            {
                'id', 'name', 'updated_at', 'ad_creations', 'max_rate',
                'genders', 'parents', 'age_ranges', 'targeting',
            }
        )

        self.assertEqual(
            set(ad_group_creation['targeting'].keys()),
            {'channel', 'video', 'topic', 'interest', 'keyword'}
        )
        self.assertEqual(len(ad_group_creation['ad_creations']), 1)
        self.assertEqual(
            set(ad_group_creation['ad_creations'][0].keys()),
            {
                'id', 'name', 'updated_at', 'tracking_template', 'final_url',
                'video_url', 'thumbnail', 'custom_params', 'display_url',
                'companion_banner',
                'video_id', 'video_title', 'video_description', 'video_thumbnail', 'video_channel_title',
            }
        )

    def test_fail_get_data_of_another_user(self):
        user = get_user_model().objects.create(
            email="another@mail.au",
        )
        AccountCreation.objects.create(
            name="", owner=user,
        )
        url = reverse("aw_creation_urls:account_creation_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'max_page',
                'items_count',
                'items',
                'current_page',
            }
        )
        self.assertEqual(response.data['items_count'], 1,
                         "Only Demo account")
        self.assertEqual(len(response.data['items']), 1)

    def test_success_get(self):
        account = Account.objects.create(id="123", name="")
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user, account=account,
        )
        camp_creation = CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            goal_units=100, max_rate="0.07",
            start=datetime.now() - timedelta(days=10),
            end=datetime.now() + timedelta(days=10),
        )
        AdGroupCreation.objects.create(
            name="", campaign_creation=camp_creation,
        )
        AdGroupCreation.objects.create(
            name="", campaign_creation=camp_creation,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation, campaign=None,
        )
        # --
        url = reverse("aw_creation_urls:account_creation_list")
        with patch(
            "aw_creation.api.serializers.SingleDatabaseApiConnector",
            new=SingleDatabaseApiConnectorPatcher
        ):
            with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
            ):
                response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'max_page',
                'items_count',
                'items',
                'current_page',
            }
        )
        self.assertEqual(response.data['items_count'], 2)
        self.assertEqual(len(response.data['items']), 2)
        item = response.data['items'][1]
        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )

    # ended account cases
    def test_success_get_account_no_end_date(self):
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
        )

        url = reverse("aw_creation_urls:account_creation_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data['items_count'], 2,
            "The account has no end date that's why it's shown"
        )

    def test_hide_account_is_ended_true(self):
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user, is_ended=True
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            end=datetime.now().date() + timedelta(days=1),
        )
        # 1
        url = reverse("aw_creation_urls:account_creation_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data['items_count'], 1,
            "The account isn't shown because of the flag 'is_ended' "
        )
        # 2
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?show_closed=1".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 2)

    def test_show_hidden_account(self):
        AccountCreation.objects.create(
            name="A", owner=self.user, is_ended=True
        )
        live_account = AccountCreation.objects.create(
            name="B", owner=self.user,
        )
        base_url = reverse("aw_creation_urls:account_creation_list")
        url = "{}?{}".format(
            base_url,
            urlencode(dict(
                show_closed="1",
                sort_by="name",
            )),
        )
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 3)
        self.assertEqual(response.data['items'][1]['name'],
                         live_account.name)

    def test_success_get_demo(self):
        url = reverse("aw_creation_urls:account_creation_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'max_page',
                'items_count',
                'items',
                'current_page',
            }
        )
        self.assertEqual(response.data['items_count'], 1)
        self.assertEqual(len(response.data['items']), 1)
        item = response.data['items'][0]

        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )
        self.assertEqual(len(item['weekly_chart']), 7)

    def test_list_no_deleted_accounts(self):
        AccountCreation.objects.create(
            name="", owner=self.user, is_deleted=True
        )
        # --
        url = reverse("aw_creation_urls:account_creation_list")
        with patch(
            "aw_reporting.demo.models.SingleDatabaseApiConnector",
            new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 1)
        self.assertEqual(len(response.data['items']), 1)

    def test_filter_campaigns_count(self):
        AccountCreation.objects.create(name="", owner=self.user)
        ac = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac, name="")
        # --
        url = reverse("aw_creation_urls:account_creation_list")
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get("{}?min_campaigns_count=1&max_campaigns_count=1".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)
        self.assertEqual(response.data['items'][0]['id'], ac.id)

    def test_filter_start_date(self):
        ac = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac, name="", start="2017-01-10")

        ac2 = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac2, name="", start="2017-02-10")
        # --
        url = reverse("aw_creation_urls:account_creation_list")
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get("{}?min_start=2017-01-01&max_start=2017-01-31".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)
        self.assertEqual(response.data['items'][0]['id'], ac.id)

    def test_filter_end_date(self):
        ac = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac, name="", end="2017-01-10")

        ac2 = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac2, name="", end="2017-02-10")
        # --
        url = reverse("aw_creation_urls:account_creation_list")
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get("{}?min_end=2017-01-01&max_end=2017-01-31".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)
        self.assertEqual(response.data['items'][0]['id'], ac.id)

    def test_success_get_import_historical_accounts(self):
        from aw_reporting.models import AWConnection, AWConnectionToUserRelation, AWAccountPermission, Account

        connection = AWConnection.objects.create(
            email="test@gmail.com",
            refresh_token="1/stxUUgC2fNCe-z1al",
        )
        AWConnectionToUserRelation.objects.create(
            user=self.user,
            connection=connection,
        )
        manager = Account.objects.create(id="1", name="")
        AWAccountPermission.objects.get_or_create(
            aw_connection=connection, account=manager,
        )
        account = Account.objects.create(id="2", name="Weird name")
        account.managers.add(manager)

        # create a few users that also can see it
        for i in range(3):
            user = get_user_model().objects.create(
                email="another{}@mail.au".format(i),
            )
            AccountCreation.objects.create(
                name="", owner=user, account=account,
            )

        # and create an usual running account creation
        created_account = Account.objects.create(id="3", name="")
        created_account.managers.add(manager)
        AccountCreation.objects.create(name="", owner=self.user, account=created_account)

        # --
        url = reverse("aw_creation_urls:account_creation_list")
        with patch(
            "aw_creation.api.serializers.SingleDatabaseApiConnector",
            new=SingleDatabaseApiConnectorPatcher
        ):
            with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
            ):
                response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'max_page',
                'items_count',
                'items',
                'current_page',
            }
        )
        self.assertEqual(response.data['items_count'], 3)
        self.assertEqual(len(response.data['items']), 3)
        item = response.data['items'][1]
        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )
        self.assertEqual(item['is_managed'], False)
