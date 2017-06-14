from datetime import datetime, timedelta

from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from urllib.parse import urlencode
from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class AccountListAPITestCase(ExtendedAPITestCase):

    details_keys = {
        'id', 'name',
        'status', 'start', 'end', 'is_optimization_active', 'is_changed',
        'creative_count', 'keywords_count', 'videos_count', 'goal_units',
        'channels_count', 'campaigns_count', 'ad_groups_count', 'read_only',
        "weekly_chart",
        'is_ended',
        'is_approved',
        'structure',
        'bidding_type',
        'video_ad_format',
        'delivery_method',
        'video_networks',
        'goal_type',
        'is_paused',
        'type',
        'goal_charts',
        'creative',
    }

    def setUp(self):
        self.user = self.create_test_user()

    def test_fail_get_data_of_another_user(self):
        user = get_user_model().objects.create(
            email="another@mail.au",
        )
        AccountCreation.objects.create(
            name="", owner=user,
        )
        url = reverse("aw_creation_urls:optimization_account_list")
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
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        camp_creation = CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            goal_units=100, max_rate="0.07",
            start=datetime.now() - timedelta(days=10),
            end=datetime.now() + timedelta(days=10),
        )
        AdGroupCreation.objects.create(
            name="", campaign_creation=camp_creation,
            max_rate="0.07",
        )
        AdGroupCreation.objects.create(
            name="", campaign_creation=camp_creation,
            max_rate="0.05",
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation, campaign=None,
        )
        # --
        url = reverse("aw_creation_urls:optimization_account_list")
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

        url = reverse("aw_creation_urls:optimization_account_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data['items_count'], 2,
            "The account has no end date that's why it's shown"
        )
        self.assertEqual(
            response.data['items'][1]['status'], "Running",
            "There is no any better status for this case"
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
        url = reverse("aw_creation_urls:optimization_account_list")
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
        self.assertEqual(response.data['items'][1]['status'], "Ended")

    def test_show_hidden_account(self):
        AccountCreation.objects.create(
            name="A", owner=self.user, is_ended=True
        )
        live_account = AccountCreation.objects.create(
            name="B", owner=self.user,
        )
        base_url = reverse("aw_creation_urls:optimization_account_list")
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
        url = reverse("aw_creation_urls:optimization_account_list")
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
        url = reverse("aw_creation_urls:optimization_account_list")
        with patch(
            "aw_reporting.demo.models.SingleDatabaseApiConnector",
            new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 1)
        self.assertEqual(len(response.data['items']), 1)

    def test_filter_status(self):
        AccountCreation.objects.create(name="", owner=self.user)
        AccountCreation.objects.create(name="", owner=self.user, is_paused=True)
        # --
        url = reverse("aw_creation_urls:optimization_account_list")
        status = "Paused"
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get("{}?status={}".format(url, status))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)
        for i in response.data['items']:
            self.assertEqual(i['status'], status)

    def test_filter_goal_units(self):
        AccountCreation.objects.create(name="", owner=self.user)
        ac = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac, name="", goal_units=100)
        # --
        url = reverse("aw_creation_urls:optimization_account_list")
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get("{}?min_goal_units=10&max_goal_units=1000".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)
        self.assertEqual(response.data['items'][0]['id'], ac.id)

    def test_filter_campaigns_count(self):
        AccountCreation.objects.create(name="", owner=self.user)
        ac = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac, name="")
        # --
        url = reverse("aw_creation_urls:optimization_account_list")
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get("{}?min_campaigns_count=1&max_campaigns_count=1".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)
        self.assertEqual(response.data['items'][0]['id'], ac.id)

    def test_filter_is_changed(self):
        AccountCreation.objects.create(name="", owner=self.user)
        ac = AccountCreation.objects.create(name="", owner=self.user)
        AccountCreation.objects.filter(id=ac.pk).update(is_changed=False)
        # --
        url = reverse("aw_creation_urls:optimization_account_list")
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get("{}?is_changed=0".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 2)
        for i in response.data['items']:
            self.assertEqual(i['is_changed'], False)

        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get("{}?is_changed=1".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)
        for i in response.data['items']:
            self.assertEqual(i['is_changed'], True)

    def test_filter_start_date(self):
        ac = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac, name="", start="2017-01-10")

        ac2 = AccountCreation.objects.create(name="", owner=self.user)
        CampaignCreation.objects.create(account_creation=ac2, name="", start="2017-02-10")
        # --
        url = reverse("aw_creation_urls:optimization_account_list")
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
        url = reverse("aw_creation_urls:optimization_account_list")
        with patch(
                "aw_reporting.demo.models.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get("{}?min_end=2017-01-01&max_end=2017-01-31".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)
        self.assertEqual(response.data['items'][0]['id'], ac.id)
