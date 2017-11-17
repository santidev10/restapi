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
        'id', 'name', 'account', 'status', 'start', 'end', 'is_managed',
        'is_changed', 'weekly_chart', 'thumbnail',
        'video_views', 'cost', 'video_view_rate', 'ctr_v', 'impressions', 'clicks',
        "ad_count", "channel_count", "video_count", "interest_count", "topic_count", "keyword_count",
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
                'id', 'name', 'account', 'campaign_creations',
                'updated_at', 'is_ended', 'is_paused', 'is_approved',
            }
        )

        item = AccountCreation.objects.get(pk=response.data['id'])
        self.assertEqual(item.is_deleted, True)  # item is hidden

        campaign_creation = response.data['campaign_creations'][0]
        self.assertEqual(
            set(campaign_creation.keys()),
            {
                'id', 'name', 'updated_at', 'content_exclusions',
                'start', 'end', 'budget', 'languages', 'devices',
                'frequency_capping', 'ad_schedule_rules',
                'location_rules', 'ad_group_creations',
                'video_networks', 'type', 'delivery_method',
            }
        )
        self.assertEqual(len(campaign_creation['languages']), 1)

        ad_group_creation = campaign_creation['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_creation.keys()),
            {
                'id', 'name', 'updated_at', 'ad_creations', 'max_rate',
                'genders', 'parents', 'age_ranges', 'targeting', 'video_ad_format',
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
                'video_url', 'video_ad_format', 'custom_params', 'display_url', 'companion_banner',
                'video_id', 'video_title', 'video_description', 'video_thumbnail',
                'video_channel_title', 'video_duration',
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
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        creative1 = VideoCreative.objects.create(id="SkubJruRo8w")
        creative2 = VideoCreative.objects.create(id="siFHgF9TOVA")
        date = datetime.now()
        VideoCreativeStatistic.objects.create(creative=creative1, date=date, ad_group=ad_group, impressions=10)
        VideoCreativeStatistic.objects.create(creative=creative2, date=date, ad_group=ad_group, impressions=12)

        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user, account=account,
        )
        camp_creation = CampaignCreation.objects.create(
            name="", account_creation=ac_creation,
            goal_units=100, max_rate="0.07",
            start=datetime.now() - timedelta(days=10),
            end=datetime.now() + timedelta(days=10),
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=camp_creation,
        )
        AdCreation.objects.create(name="", ad_group_creation=ad_group_creation,
                                  video_thumbnail="http://some.url.com")
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

    def test_success_sort_by(self):
        account1 = Account.objects.create(id="123", name="")
        stats = dict(account=account1,  name="", impressions=10, video_views=9, clicks=9, cost=9)
        Campaign.objects.create(id=1, **stats)
        Campaign.objects.create(id=2, **stats)
        top_account = AccountCreation.objects.create(
            name="Top account", owner=self.user, account=account1,
        )

        account2 = Account.objects.create(id="456", name="")
        stats = dict(account=account2, name="", impressions=3, video_views=2, clicks=1, cost=3)
        Campaign.objects.create(id=3, **stats)
        Campaign.objects.create(id=4, **stats)
        AccountCreation.objects.create(
            name="Bottom account", owner=self.user, account=account2,
        )

        # --
        url = reverse("aw_creation_urls:account_creation_list")

        for sort_by in ("impressions", "video_views", "clicks", "cost", "video_view_rate", "ctr_v"):
            with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
            ):
                with patch(
                    "aw_reporting.demo.models.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
                ):
                    response = self.client.get("{}?sort_by={}".format(url, sort_by))
            self.assertEqual(response.status_code, HTTP_200_OK)
            items = response.data['items']
            expected_top_account = items[1]
            self.assertEqual(top_account.name, expected_top_account['name'])

    def test_success_sort_by_name(self):
        account1 = Account.objects.create(id="123", name="")
        creation_1 = AccountCreation.objects.create(
            name="First account", owner=self.user, account=account1,
        )

        account2 = Account.objects.create(id="456", name="Second account")
        creation_2 = AccountCreation.objects.create(name="", owner=self.user, account=account2, is_managed=False)

        creation_3 = AccountCreation.objects.create(name="Third account", owner=self.user, account=account2)

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
                response = self.client.get("{}?sort_by=name".format(url))

        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data['items']

        self.assertEqual(
            ("Demo", creation_1.name, creation_2.account.name, creation_3.name),
            tuple(a["name"] for a in items)
        )

    def test_success_metrics_filter(self):
        AccountCreation.objects.create(name="Empty", owner=self.user,
                                       is_ended=False, is_paused=False, is_approved=True)
        account = Account.objects.create(id=1, name="")
        AccountCreation.objects.create(
            name="Maximum", owner=self.user, account=account,
        )
        Campaign.objects.create(
            id=1, name="", account=account,
            impressions=10, video_views=10, clicks=10, cost=10,
        )
        account = Account.objects.create(id=2, name="")
        AccountCreation.objects.create(
            name="Minimum", owner=self.user, account=account,
        )
        Campaign.objects.create(
            id=2, name="", account=account,
            impressions=4, video_views=2, clicks=1, cost=1,
        )
        # --
        test_filters = (
            ("impressions", 1, 5, 2, 11),
            ("video_views", 1, 2, 3, 11),
            ("clicks", 1, 2, 2, 10),
            ("cost", 1, 2, 2, 10),
            ("video_view_rate", 50, 75, 75, 100),
            ("ctr_v", 25, 50, 75, 100),
        )
        base_url = reverse("aw_creation_urls:account_creation_list")

        for metric, min1, max1, min2, max2 in test_filters:
            with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
            ):
                with patch(
                    "aw_reporting.demo.models.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
                ):
                    response = self.client.get(
                        "{base_url}?min_{metric}={min}&max_{metric}={max}".format(
                            base_url=base_url, metric=metric, min=min1, max=max1)
                    )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data['items'][-1]['name'], "Minimum")
            for item in response.data['items']:
                self.assertGreaterEqual(item[metric], min1)
                self.assertLessEqual(item[metric], max1)

            with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
            ):
                with patch(
                    "aw_reporting.demo.models.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
                ):
                    response = self.client.get(
                        "{base_url}?min_{metric}={min}&max_{metric}={max}".format(
                            base_url=base_url, metric=metric, min=min2, max=max2)
                    )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data['items'][-1]['name'], "Maximum")
            for item in response.data['items']:
                self.assertGreaterEqual(item[metric], min2)
                self.assertLessEqual(item[metric], max2)

    def test_success_status_filter(self):
        AccountCreation.objects.create(name="Pending", owner=self.user)
        AccountCreation.objects.create(name="Ended", owner=self.user,
                                       is_ended=True, is_paused=True, is_approved=True)
        AccountCreation.objects.create(name="Paused", owner=self.user,
                                       is_ended=False, is_paused=True, is_approved=True)
        AccountCreation.objects.create(name="Approved", owner=self.user,
                                       is_ended=False, is_paused=False, is_approved=True)
        AccountCreation.objects.create(
            name="Running", owner=self.user, sync_at=datetime.now(),
        )
        account = Account.objects.create(id="111", name="From AdWords")
        AccountCreation.objects.create(
            name="", owner=self.user, is_managed=False, account=account,
        )
        # --
        expected = (
            ("From AdWords", 1),
            ("Pending", 1),
            ("Ended", 1),
            ("Paused", 1),
            ("Approved", 1),
            ("Running", 2),  # +DemoAccount
        )
        base_url = reverse("aw_creation_urls:account_creation_list")

        for status, count in expected:
            with patch(
                    "aw_creation.api.serializers.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
            ):
                with patch(
                        "aw_reporting.demo.models.SingleDatabaseApiConnector",
                        new=SingleDatabaseApiConnectorPatcher
                ):
                    response = self.client.get("{}?show_closed=1&status={}".format(base_url, status))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data['items_count'], count)
            self.assertEqual(response.data['items'][-1]['name'], status)
            for i in response.data['items']:
                self.assertEqual(i['status'], status)

    def test_success_dates_filter(self):
        today = datetime(2015, 1, 1).date()
        max_end, min_start = today, today - timedelta(days=10)

        AccountCreation.objects.create(name="Empty", owner=self.user)
        ac = AccountCreation.objects.create(name="Settings+", owner=self.user)
        CampaignCreation.objects.create(name="", account_creation=ac, start=min_start, end=max_end)

        ac = AccountCreation.objects.create(name="Settings-", owner=self.user)
        CampaignCreation.objects.create(name="", account_creation=ac, start=min_start, end=max_end + timedelta(days=1))

        account = Account.objects.create(id=1, name="")
        Campaign.objects.create(id=1, name="", account=account, start_date=min_start, end_date=max_end)
        AccountCreation.objects.create(name="Improted+", owner=self.user, account=account)

        account = Account.objects.create(id=2, name="")
        Campaign.objects.create(id=2, name="", account=account,
                                start_date=min_start - timedelta(days=1), end_date=max_end)
        AccountCreation.objects.create(name="Improted-", owner=self.user, account=account)

        base_url = reverse("aw_creation_urls:account_creation_list")

        with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            with patch(
                    "aw_reporting.demo.models.SingleDatabaseApiConnector",
                    new=SingleDatabaseApiConnectorPatcher
            ):
                response = self.client.get("{}?min_start={}&max_end={}".format(base_url, min_start, max_end))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], 2)
        for item in response.data['items']:
            self.assertIs(item['name'].endswith("+"), True)

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

    def test_filter_campaigns_count_from_ad_words(self):
        account = Account.objects.create(id=1, name="")
        Campaign.objects.create(id=1, name="", account=account)
        ac = AccountCreation.objects.create(name="", account=account, owner=self.user, is_managed=False)
        AccountCreation.objects.create(name="", owner=self.user)
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
        self.assertEqual(item['name'], account.name)
        self.assertEqual(item['is_managed'], False)
