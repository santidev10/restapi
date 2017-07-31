from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_creation.models import AccountCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID, DemoAccount
from aw_reporting.models import Account, Campaign, AdGroup, YTChannelStatistic
from saas.utils_tests import ExtendedAPITestCase
from datetime import datetime
import json


class AccountNamesAPITestCase(ExtendedAPITestCase):

    data_keys = {
        "id", "name", "status", 'start_date', 'end_date',
        'average_cpv', 'cost', 'video_impressions', 'ctr_v', 'clicks',
        'video_views', 'ctr', 'impressions', 'video_view_rate', 'average_cpm',
    }

    def test_success_get(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user, account=account)

        start = datetime(2017, 1, 1).date()
        end = datetime(2017, 1, 2).date()
        campaign = Campaign.objects.create(id="1", name="Campaign wow", status="eligible",
                                           account=account, start_date=start, end_date=end)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)

        stats = dict(
            yt_id="AAA",
            ad_group=ad_group,
            impressions=10,
            video_views=5,
            clicks=1,
            cost=1,
        )
        YTChannelStatistic.objects.create(date=start, **stats)
        YTChannelStatistic.objects.create(date=end, **stats)

        url = reverse("aw_creation_urls:performance_targeting_report",
                      args=(account_creation.id, "channel"))

        response = self.client.post(
            url, json.dumps(dict(
                start_date=str(start),
                end_date=str(start),
                campaigns=[campaign.id],
                ad_groups=[ad_group.id],
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
        self.assertEqual(campaign_data['id'], campaign.id)
        self.assertEqual(campaign_data['name'], campaign.name)
        self.assertEqual(campaign_data['start_date'], campaign.start_date)
        self.assertEqual(campaign_data['end_date'], campaign.end_date)
        self.assertEqual(campaign_data['status'], campaign.status)
        self.assertEqual(campaign_data['average_cpv'], .2)
        self.assertEqual(campaign_data['video_views'], 5)

    def test_success_get_demo(self):
        self.create_test_user()
        url = reverse("aw_creation_urls:performance_targeting_report",
                      args=(DEMO_ACCOUNT_ID, "channel"))
        account = DemoAccount()
        campaign = account.children[0]
        response = self.client.post(
            url, json.dumps(dict(
                campaigns=[campaign.id]
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
        self.assertEqual(campaign_data['id'], campaign.id)
        self.assertEqual(campaign_data['name'], campaign.name)
        self.assertEqual(campaign_data['start_date'], campaign.start_date)
        self.assertEqual(campaign_data['end_date'], campaign.end_date)
        self.assertEqual(campaign_data['status'], campaign.status)
