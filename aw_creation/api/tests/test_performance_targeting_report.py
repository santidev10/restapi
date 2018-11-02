import json
from datetime import datetime
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.models import AccountCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models import TargetingItem
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.demo.models import DemoAccount
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class PerformanceReportAPITestCase(ExtendedAPITestCase):
    data_keys = {
        "label", "items", "id",
        "impressions", "video_views", "clicks", "cost",
        "average_cpv", "average_cpm", "ctr", "ctr_v", "video_view_rate",
        "kpi",
    }

    item_keys = {
        "item", "campaign", "ad_group", "targeting", "is_negative",
        "impressions", "video_views", "clicks", "cost",
        "average_cpv", "average_cpm", "ctr", "ctr_v", "video_view_rate",
        "video_clicks"
    }

    def test_success_post(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user, account=account, is_managed=False)

        start = datetime(2017, 1, 1).date()
        end = datetime(2017, 1, 2).date()
        campaign = Campaign.objects.create(id="1", name="Campaign wow", status="eligible",
                                           account=account, start_date=start, end_date=end)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign, video_views=1)

        YTChannelStatistic.objects.create(date=start, yt_id="AAA", ad_group=ad_group, video_views=5, impressions=10)
        YTChannelStatistic.objects.create(date=end, yt_id="AAA", ad_group=ad_group, video_views=5, impressions=0)

        YTVideoStatistic.objects.create(date=start, yt_id="AAA", ad_group=ad_group, video_views=2, impressions=10)
        audience, _ = Audience.objects.get_or_create(id=1, name="Auto", type=Audience.CUSTOM_AFFINITY_TYPE)
        AudienceStatistic.objects.create(date=start, audience=audience, ad_group=ad_group,
                                         video_views=2, impressions=10)
        topic, _ = Topic.objects.get_or_create(id=1, name="Demo")
        TopicStatistic.objects.create(date=start, topic=topic, ad_group=ad_group, video_views=2, impressions=10)
        KeywordStatistic.objects.create(date=start, keyword="AAA", ad_group=ad_group, video_views=2, impressions=10)

        url = reverse("aw_creation_urls:performance_targeting_report", args=(account_creation.id,))

        with patch("aw_creation.api.views.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(dict(
                    start_date=str(start),
                    end_date=str(start),
                    campaigns=[campaign.id],
                    ad_groups=[ad_group.id],
                    targeting=["topic", "interest", "keyword", "channel", "video"],
                )),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), {"reports"})
        self.assertEqual(len(data["reports"]), 1)
        report_data = data["reports"][0]
        self.assertEqual(
            set(report_data.keys()), self.data_keys
        )
        self.assertEqual(report_data['label'], "All campaigns")
        self.assertEqual(len(report_data["items"]), 5)

        for n, item in enumerate(report_data['items']):
            self.assertEqual(set(item.keys()), self.item_keys)

    def test_targeting_status(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        start, end = datetime(2017, 1, 1).date(), datetime(2017, 1, 2).date()
        campaign = Campaign.objects.create(id="999", name="Campaign wow", status="eligible",
                                           account=account, start_date=start, end_date=end)
        ad_group = AdGroup.objects.create(id="666", name="", campaign=campaign, video_views=1)

        YTChannelStatistic.objects.create(date=start, yt_id="AAA", ad_group=ad_group, video_views=5, impressions=10)
        vs = YTVideoStatistic.objects.create(date=start, yt_id="AAA", ad_group=ad_group, video_views=2, impressions=10)
        ks = KeywordStatistic.objects.create(date=start, keyword="AAA", ad_group=ad_group,
                                             video_views=2, impressions=10)

        account_creation = AccountCreation.objects.create(name="", owner=user, account=account, is_managed=False)
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation, campaign=campaign)
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation, ad_group=ad_group)
        TargetingItem.objects.create(ad_group_creation=ad_group_creation, type=TargetingItem.KEYWORD_TYPE,
                                     criteria=ks.keyword, is_negative=True)
        TargetingItem.objects.create(ad_group_creation=ad_group_creation, type=TargetingItem.VIDEO_TYPE,
                                     criteria=vs.yt_id, is_negative=False)

        url = reverse("aw_creation_urls:performance_targeting_report", args=(account_creation.id,))

        with patch("aw_creation.api.views.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(dict(
                    targeting=["keyword", "channel", "video"],
                )),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        report_data = response.data["reports"][0]
        self.assertEqual(len(report_data["items"]), 3)

        for item in report_data['items']:
            if item["targeting"] == "Channels":
                self.assertEqual(item["is_negative"], False)
            elif item["targeting"] == "Videos":
                self.assertEqual(item["is_negative"], False)
            elif item["targeting"] == "Keywords":
                self.assertEqual(item["is_negative"], True)

    def test_targeting_interest(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        start, end = datetime(2017, 1, 1).date(), datetime(2017, 1, 2).date()
        campaign = Campaign.objects.create(id="999", name="Campaign wow", status="eligible",
                                           account=account, start_date=start, end_date=end)
        ad_group = AdGroup.objects.create(id="666", name="", campaign=campaign, video_views=1)

        audience = Audience.objects.create(id=1, name="Test", type=Audience.AFFINITY_TYPE)
        AudienceStatistic.objects.create(date=start, audience=audience, ad_group=ad_group, video_views=5,
                                         impressions=10)

        account_creation = AccountCreation.objects.create(name="", owner=user, account=account, is_managed=False)
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation, campaign=campaign)
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation, ad_group=ad_group)
        TargetingItem.objects.create(ad_group_creation=ad_group_creation, type=TargetingItem.INTEREST_TYPE,
                                     criteria=audience.id, is_negative=True)

        url = reverse("aw_creation_urls:performance_targeting_report", args=(account_creation.id,))

        with patch("aw_creation.api.views.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(dict(
                    targeting=["interest"],
                )),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        report_data = response.data["reports"][0]
        self.assertEqual(len(report_data["items"]), 1)

        for item in report_data['items']:
            self.assertEqual(item["is_negative"], True)

    def test_success_group_by_campaign(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user, account=account, is_managed=False)

        start = datetime(2017, 1, 1).date()
        end = datetime(2017, 1, 2).date()
        campaign1 = Campaign.objects.create(id="1", name="A campaign", status="eligible",
                                            account=account, start_date=start, end_date=end)
        ad_group1 = AdGroup.objects.create(id=1, name="", campaign=campaign1, video_views=1)
        campaign2 = Campaign.objects.create(id="2", name="B campaign", status="eligible",
                                            account=account, start_date=start, end_date=end)
        ad_group2 = AdGroup.objects.create(id=2, name="", campaign=campaign2, video_views=1)

        YTChannelStatistic.objects.create(date=start, yt_id="AAA", ad_group=ad_group1, video_views=5, impressions=10)
        YTChannelStatistic.objects.create(date=end, yt_id="AAA", ad_group=ad_group1, video_views=5, impressions=0)

        YTVideoStatistic.objects.create(date=start, yt_id="AAA", ad_group=ad_group1, video_views=2, impressions=10)
        audience, _ = Audience.objects.get_or_create(id=1, name="Auto", type=Audience.CUSTOM_AFFINITY_TYPE)
        AudienceStatistic.objects.create(date=start, audience=audience, ad_group=ad_group2,
                                         video_views=2, impressions=10)
        topic, _ = Topic.objects.get_or_create(id=1, name="Demo")
        TopicStatistic.objects.create(date=start, topic=topic, ad_group=ad_group2, video_views=2, impressions=10)
        KeywordStatistic.objects.create(date=start, keyword="AAA", ad_group=ad_group2, video_views=2, impressions=10)

        url = reverse("aw_creation_urls:performance_targeting_report", args=(account_creation.id,))

        with patch("aw_creation.api.views.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(dict(
                    start_date=str(start),
                    end_date=str(start),
                    group_by="campaign",
                    targeting=["topic", "interest", "keyword", "channel", "video"],
                )),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data["reports"]
        self.assertEqual(len(data), 2)

        self.assertEqual(set(data[0].keys()), self.data_keys)
        self.assertEqual(data[0]['label'], "A campaign")
        self.assertEqual(len(data[0]["items"]), 2)
        for item in data[0]['items']:
            self.assertEqual(set(item.keys()), self.item_keys)

        self.assertEqual(set(data[0].keys()), self.data_keys)
        self.assertEqual(data[1]['label'], "B campaign")
        self.assertEqual(len(data[1]["items"]), 3)
        for item in data[0]['items']:
            self.assertEqual(set(item.keys()), self.item_keys)

    def test_success_min_max_kpi(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            name="", owner=user, account=account, is_managed=False)

        start = datetime(2017, 1, 1).date()
        end = datetime(2017, 1, 2).date()
        campaign = Campaign.objects.create(
            id="1", name="A campaign", status="eligible",
            account=account, start_date=start, end_date=end)
        ad_group = AdGroup.objects.create(
            id=1, name="", campaign=campaign, video_views=1)

        YTChannelStatistic.objects.create(
            date=start, yt_id="AAA", ad_group=ad_group,
            video_views=2, impressions=8, clicks=1, cost=2,
        )
        YTVideoStatistic.objects.create(
            date=start, yt_id="AAA", ad_group=ad_group,
            video_views=2, impressions=8, clicks=1, cost=2,
        )
        audience, _ = Audience.objects.get_or_create(
            id=1, name="Auto", type=Audience.CUSTOM_AFFINITY_TYPE)
        AudienceStatistic.objects.create(
            date=start, audience=audience, ad_group=ad_group,
            video_views=2, impressions=8, clicks=1, cost=2,
        )
        # top ctr 25%
        topic, _ = Topic.objects.get_or_create(id=1, name="Demo")
        TopicStatistic.objects.create(
            date=start, topic=topic, ad_group=ad_group,
            video_views=2, impressions=8, clicks=2, cost=2,
        )
        # top video_view_rate 75%
        KeywordStatistic.objects.create(
            date=start, keyword="AAA", ad_group=ad_group,
            video_views=6, impressions=8, clicks=1, cost=2,
        )

        url = reverse(
            "aw_creation_urls:performance_targeting_report",
            args=(account_creation.id,))

        with patch("aw_creation.api.views.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(dict(
                    start_date=str(start),
                    end_date=str(start),
                    group_by="campaign",
                    targeting=[
                        "topic", "interest", "keyword", "channel", "video"],
                )),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        kpi = response.data["reports"][0]["kpi"]

        self.assertEqual(kpi["video_view_rate"]["min"], 25)
        self.assertEqual(kpi["video_view_rate"]["max"], 75)

        self.assertEqual(kpi["ctr"]["min"], 12.5)
        self.assertEqual(kpi["ctr"]["max"], 25)
        self.assertAlmostEqual(kpi["ctr_v"]["min"], 1 / 6 * 100, places=10)
        self.assertEqual(kpi["ctr_v"]["max"], 100)

        self.assertAlmostEqual(kpi["average_cpv"]["min"], 1 / 3, places=10)
        self.assertEqual(kpi["average_cpv"]["max"], 1)

        self.assertEqual(kpi["average_cpm"]["min"], 250)
        self.assertEqual(kpi["average_cpm"]["max"], 250)

    def test_success_post_demo(self):
        self.create_test_user()
        url = reverse("aw_creation_urls:performance_targeting_report",
                      args=(DEMO_ACCOUNT_ID,))
        account = DemoAccount()
        campaign = account.children[0]
        ad_group = campaign.children[0]

        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(dict(
                    campaigns=[campaign.id],
                    ad_groups=[ad_group.id],
                    group_by="campaign",
                    targeting=["channel"],
                )),
                content_type='application/json',
            )

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), {"reports"})
        self.assertEqual(len(data["reports"]), 1)
        campaign_data = data["reports"][0]

        self.assertEqual(
            set(campaign_data.keys()),
            self.data_keys
        )
        self.assertEqual(campaign_data['label'], campaign.name)
        self.assertEqual(len(campaign_data['items']), 12)

        item = campaign_data['items'][0]
        self.assertEqual(set(item.keys()), self.item_keys)
