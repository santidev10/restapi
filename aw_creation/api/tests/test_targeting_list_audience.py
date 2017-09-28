from urllib.parse import urlencode
from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from rest_framework.authtoken.models import Token
from aw_creation.models import *
from aw_reporting.models import Audience
from saas.utils_tests import ExtendedAPITestCase


class InterestTargetingListTestCase(ExtendedAPITestCase):

    def create_ad_group(self, user):
        account = AccountCreation.objects.create(
            id="1", name="", owner=user,
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
        user = self.create_test_user(auth=False)
        ad_group = self.create_ad_group(user)
        for i in range(10):
            Audience.objects.create(
                id=i, name="Interest#{}".format(i),
                type=Audience.IN_MARKET_TYPE,
            )
            TargetingItem.objects.create(
                criteria=i,
                ad_group_creation=ad_group,
                type=TargetingItem.INTEREST_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:ad_group_creation_targeting_export",
            args=(ad_group.id, TargetingItem.INTEREST_TYPE, "positive"),
        )
        response = self.client.get(url)
        self.assertIn(response.status_code,
                      (HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED))

        token = Token.objects.create(user=user)
        url = "{}?{}".format(
            str(url),
            urlencode({'auth_token': token.key}),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertEqual(len(lines), 6)



