from urllib.parse import urlencode

from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from aw_creation.models import *
from utils.utittests.test_case import ExtendedAPITestCase


class KeywordTargetingListTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_ad_group(self):
        account = AccountCreation.objects.create(
            id="1", name="", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            account_creation=account, name="",
        )
        ad_group_creation = AdGroupCreation.objects.create(
            id="1", name="", max_rate=0.01,
            campaign_creation=campaign_creation,
        )
        AccountCreation.objects.filter(pk=account.id).update(sync_at=timezone.now())
        account.refresh_from_db()
        self.assertEqual(account.is_changed, False)
        return ad_group_creation

    def test_export_list(self):
        ad_group = self.create_ad_group()
        for i in range(10):
            TargetingItem.objects.create(
                criteria="KW#{}".format(i),
                ad_group_creation=ad_group,
                type=TargetingItem.KEYWORD_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:ad_group_creation_targeting_export",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE, "positive"),
        )
        url = "{}?{}".format(
            str(url),
            urlencode({'auth_token': self.user.auth_token.key}),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertEqual(len(lines), 6)