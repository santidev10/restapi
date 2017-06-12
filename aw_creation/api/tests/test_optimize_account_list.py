from datetime import datetime, timedelta

from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from urllib.parse import urlencode
from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from aw_reporting.api.tests.base import AwReportingAPITestCase


class AccountListAPITestCase(AwReportingAPITestCase):

    details_keys = {
        'id', 'name', 'read_only',
        'status', 'start', 'end', 'is_optimization_active', 'is_changed',
        'impressions', 'views', 'cost', 'campaigns_count',

        "goal_type",
        "bidding_type",
        "video_ad_format",
        "video_networks",
        "type",
        "delivery_method",

        "creative",
        "structure",
        "goal_charts",
        "weekly_chart",
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

    def test_hide_account_end_in_past(self):
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            end=datetime.now().date() - timedelta(days=1),
        )

        # 1
        url = reverse("aw_creation_urls:optimization_account_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data['items_count'], 1,
            "The only campaign with end date ended yesterday"
        )
        # 2
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?show_closed=1".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 2)
        self.assertEqual(response.data['items'][1]['status'], "Ended")

    def test_hide_account_end_in_past_two_campaigns(self):
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
        )
        CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            end=datetime.now().date() - timedelta(days=1),
        )
        # 1
        url = reverse("aw_creation_urls:optimization_account_list")
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data['items_count'], 1,
            "The account isn't shown because the only date is in the past"
        )
        # 2
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get("{}?show_closed=1".format(url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 2)
        self.assertEqual(response.data['items'][1]['status'], "Ended")

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
        self.assertEqual(
            item['video_ad_format'],
            dict(id=AccountCreation.IN_STREAM_TYPE,
                 name=AccountCreation.VIDEO_AD_FORMATS[0][1]),
        )
        self.assertEqual(
            item['type'],
            dict(id=AccountCreation.VIDEO_TYPE,
                 name=AccountCreation.CAMPAIGN_TYPES[0][1]),
        )
        self.assertEqual(
            item['goal_type'],
            dict(id=AccountCreation.GOAL_VIDEO_VIEWS,
                 name=AccountCreation.GOAL_TYPES[0][1]),
        )
        self.assertEqual(
            item['delivery_method'],
            dict(id=AccountCreation.STANDARD_DELIVERY,
                 name=AccountCreation.DELIVERY_METHODS[0][1]),
        )
        self.assertEqual(
            item['bidding_type'],
            dict(id=AccountCreation.MANUAL_CPV_BIDDING,
                 name=AccountCreation.BIDDING_TYPES[0][1]),
        )
        self.assertEqual(
            item['video_networks'],
            [dict(id=uid, name=n)
             for uid, n in AccountCreation.VIDEO_NETWORKS],
        )
        self.assertEqual(
            set(item['creative'].keys()),
            {'id', 'name', 'thumbnail'}
        )
        self.assertEqual(len(item['goal_charts']), 2)
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

    def test_success_get_readonly_accounts(self):
        self.create_account(self.user)

        another_user = get_user_model().objects.create(email="another@mail.au")
        self.create_account(another_user)

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
        self.assertEqual(len(response.data['items']), 2)
        item = response.data['items'][1]
        self.assertEqual(
            set(item.keys()),
            self.details_keys,
        )
        self.assertEqual(item['read_only'], True)
