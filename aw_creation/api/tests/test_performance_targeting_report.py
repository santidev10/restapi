from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_creation.models import AccountCreation, CampaignCreation, AdGroupCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID, DemoAccount
from aw_reporting.models import Account, Campaign, AdGroup, YTChannelStatistic, Audience, Topic, \
    AWConnectionToUserRelation, AWConnection, YTVideoStatistic, AudienceStatistic, TopicStatistic, KeywordStatistic
from saas.utils_tests import ExtendedAPITestCase
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from datetime import datetime
import json


class PerformanceReportAPITestCase(ExtendedAPITestCase):

    data_keys = {
        "label", "items", "passes",

        "impressions", "video_views", "clicks", "cost",
        "average_cpv", "average_cpm", "ctr", "ctr_v", "video_view_rate",
    }

    item_keys = {
        "item", "campaign", "ad_group", "passes", "targeting",

        "impressions", "video_views", "clicks", "cost",
        "average_cpv", "average_cpm", "ctr", "ctr_v", "video_view_rate",
    }

    def test_success_post(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
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
                    video_view_rate=30,
                )),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        report_data = data[0]
        self.assertEqual(
            set(report_data.keys()), self.data_keys
        )
        self.assertEqual(report_data['label'], "All campaigns")
        self.assertEqual(len(report_data["items"]), 5)

        for n, item in enumerate(report_data['items']):
            self.assertEqual(set(item.keys()), self.item_keys)

            if item["targeting"] == "Channels":
                self.assertEqual(n, 0, "First item in the list")
                self.assertEqual(item['passes'], True)
                self.assertEqual(item['video_view_rate']['value'], 50)
                self.assertEqual(item['video_view_rate']['passes'], True)

            else:
                self.assertEqual(item['passes'], False)
                self.assertEqual(item['video_view_rate']['value'], 20)
                self.assertEqual(item['video_view_rate']['passes'], False)

    def test_success_group_by_campaign(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
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
                    video_view_rate=30,
                )),
                content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
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
                    video_view_rate=30,
                )),
                content_type='application/json',
            )

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data), 1)
        campaign_data = data[0]

        self.assertEqual(
            set(campaign_data.keys()),
            self.data_keys
        )
        self.assertEqual(campaign_data['label'], campaign.name)
        self.assertEqual(len(campaign_data['items']), 20)

        item = campaign_data['items'][0]
        self.assertEqual(set(item.keys()), self.item_keys)
