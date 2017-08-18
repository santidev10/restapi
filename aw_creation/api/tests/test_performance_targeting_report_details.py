from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_creation.models import AccountCreation, CampaignCreation, AdGroupCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID, DemoAccount
from aw_reporting.models import Account, Campaign, AdGroup, YTChannelStatistic, KeywordStatistic, YTVideoStatistic,\
    AudienceStatistic, TopicStatistic, Audience, Topic
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from datetime import datetime
from aw_reporting.api.tests.base import AwReportingAPITestCase
import json


class AccountNamesAPITestCase(AwReportingAPITestCase):

    data_keys = {
        "id", "name",
        'average_cpv', 'cost', 'video_impressions', 'ctr_v', 'clicks',
        'video_views', 'ctr', 'impressions', 'video_view_rate', 'average_cpm',
    }

    def test_success_post_all_dimensions(self):
        user = self.create_test_user()
        account = self.create_account(user)
        account_creation = AccountCreation.objects.create(id="1", name="", owner=user)
        campaign = Campaign.objects.create(id=1, name="", account=account)
        campaign_creation = CampaignCreation.objects.create(name="", campaign=campaign,
                                                            account_creation=account_creation)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        ag_creation = AdGroupCreation.objects.create(id=444, name="", ad_group=ad_group,
                                                     campaign_creation=campaign_creation)

        date = datetime(2017, 1, 1).date()
        stats = dict(
            date=date, ad_group=ad_group,
            impressions=10, video_views=5, clicks=1, cost=1,
        )
        KeywordStatistic.objects.create(keyword="blow", **stats)
        YTChannelStatistic.objects.create(yt_id="UC-lHJZR3Gqxm24_Vd_AJ5Yw", **stats)
        YTVideoStatistic.objects.create(yt_id="9bZkp7q19f0", **stats)
        topic, _ = Topic.objects.get_or_create(name="AC")
        TopicStatistic.objects.create(topic=topic, **stats)
        audience = Audience.objects.create(name="What", type=Audience.CUSTOM_AFFINITY_TYPE)
        AudienceStatistic.objects.create(audience=audience, **stats)

        with patch("aw_creation.api.views.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            for dimension in ("channel", "video", "keyword", "interest", "topic"):
                url = reverse("aw_creation_urls:performance_targeting_report_details",
                              args=(ag_creation.id, dimension))
                response = self.client.post(url)
                self.assertEqual(response.status_code, HTTP_200_OK)
                data = response.data
                self.assertEqual(len(data), 1)
                keys = set(self.data_keys)
                if dimension in ("channel", "video"):
                    keys.add("thumbnail")

                self.assertEqual(set(data[0].keys()), keys)
                self.assertEqual(set(data[0]['video_view_rate'].keys()), {"value", "passes"})

    def test_success_post_demo(self):
        self.create_test_user()
        account = DemoAccount()
        campaign = account.children[0]
        ad_group = campaign.children[0]

        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            for dimension in ("channel", "video", "keyword", "interest", "topic"):
                url = reverse("aw_creation_urls:performance_targeting_report_details",
                              args=(ad_group.id, dimension))
                response = self.client.post(url)

                self.assertEqual(response.status_code, HTTP_200_OK)
                data = response.data
                self.assertGreater(len(data), 1)
                keys = set(self.data_keys)
                if dimension in ("channel", "video"):
                    keys.add("thumbnail")
                if dimension == "video":
                    keys.add("duration")

                self.assertEqual(set(data[0].keys()), keys - {"video_impressions"})
                self.assertEqual(set(data[0]["video_view_rate"].keys()), {"value", "passes"})
