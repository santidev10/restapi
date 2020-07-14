import json
from datetime import datetime

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.models import AccountCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models import TargetingItem
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import KeywordStatistic
from utils.demo.recreate_test_demo_data import recreate_test_demo_data
from utils.unittests.test_case import ExtendedAPITestCase


class PerformanceItemAPITestCase(ExtendedAPITestCase):

    def test_success_change_status(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        date = datetime(2017, 1, 1).date()
        campaign = Campaign.objects.create(id="999", name="", account=account)
        ad_group = AdGroup.objects.create(id="666", name="", campaign=campaign)
        ks = KeywordStatistic.objects.create(date=date, keyword="AAA", ad_group=ad_group)

        account_creation = AccountCreation.objects.create(name="", owner=user, account=account, is_managed=False)
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation, campaign=campaign)
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation, ad_group=ad_group)
        TargetingItem.objects.create(ad_group_creation=ad_group_creation, type=TargetingItem.KEYWORD_TYPE,
                                     criteria=ks.keyword, is_negative=False)

        url = reverse("aw_creation_urls:performance_targeting_item",
                      args=("Keywords", ad_group.id, ks.keyword))

        response = self.client.patch(
            url, json.dumps(dict(
                is_negative=True,
            )),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_fail_change_status_no_targeting_item(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        date = datetime(2017, 1, 1).date()
        campaign = Campaign.objects.create(id="999", name="", account=account)
        ad_group = AdGroup.objects.create(id="666", name="", campaign=campaign)
        ks = KeywordStatistic.objects.create(date=date, keyword="AAA", ad_group=ad_group)

        account_creation = AccountCreation.objects.create(name="", owner=user, account=account, is_managed=False)
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation, campaign=campaign)
        AdGroupCreation.objects.create(name="", campaign_creation=campaign_creation, ad_group=ad_group)

        url = reverse("aw_creation_urls:performance_targeting_item",
                      args=("Keywords", ad_group.id, ks.keyword))

        response = self.client.patch(
            url, json.dumps(dict(
                is_negative=True,
            )),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_fail_change_status_nothing_exists(self):
        self.create_test_user()
        account = Account.objects.create(id=1, name="")
        date = datetime(2017, 1, 1).date()
        campaign = Campaign.objects.create(id="999", name="", account=account)
        ad_group = AdGroup.objects.create(id="666", name="", campaign=campaign)
        ks = KeywordStatistic.objects.create(date=date, keyword="AAA", ad_group=ad_group)

        url = reverse("aw_creation_urls:performance_targeting_item",
                      args=("Keywords", ad_group.id, ks.keyword))

        response = self.client.patch(
            url, json.dumps(dict(
                is_negative=True,
            )),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_fail_change_status_demo(self):
        recreate_test_demo_data()
        self.create_test_user()

        ad_group = AdGroup.objects.filter(campaign__account_id=DEMO_ACCOUNT_ID).first()

        url = reverse("aw_creation_urls:performance_targeting_item",
                      args=("Keywords", ad_group.id, "something"))

        response = self.client.patch(
            url, json.dumps(dict(
                is_negative=True,
            )),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
