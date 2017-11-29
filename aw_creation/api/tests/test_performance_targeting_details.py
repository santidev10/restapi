from datetime import datetime, timedelta
from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_404_NOT_FOUND
from urllib.parse import urlencode
from aw_creation.models import *
from aw_reporting.models import *
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from aw_reporting.api.tests.base import AwReportingAPITestCase


class AccountAPITestCase(AwReportingAPITestCase):

    details_keys = {
        'id', 'name', 'account', 'status', 'start', 'end', 'is_managed',
        'is_changed', 'weekly_chart', 'thumbnail',
        'video_views', 'cost', 'video_view_rate', 'ctr_v', 'impressions', 'clicks',
        "ad_count", "channel_count", "video_count", "interest_count", "topic_count", "keyword_count",
        "is_disapproved"
    }

    def setUp(self):
        self.user = self.create_test_user()

    def test_fail_get_data_of_another_user(self):
        user = get_user_model().objects.create(
            email="another@mail.au",
        )
        ac = AccountCreation.objects.create(
            name="", owner=user,
        )
        url = reverse("aw_creation_urls:performance_targeting_details", args=(ac.id,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

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
        url = reverse("aw_creation_urls:performance_targeting_details", args=(ac_creation.id,))
        with patch(
            "aw_creation.api.serializers.SingleDatabaseApiConnector",
            new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            self.details_keys,
        )

    def test_success_get_demo(self):
        url = reverse("aw_creation_urls:performance_targeting_details", args=(DEMO_ACCOUNT_ID,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        self.assertEqual(
            set(response.data.keys()),
            self.details_keys,
        )
        self.assertEqual(len(response.data['weekly_chart']), 7)

