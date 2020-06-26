import json
from datetime import timedelta

from django.db.models import F
from django.db.models import Q
from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace as RootNamespace
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class MediaBuyingAccountSyncTestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.MediaBuying.ACCOUNT_SYNC,
            [RootNamespace.AW_CREATION, Namespace.MEDIA_BUYING],
            args=(account_creation_id,),
        )

    def test_no_permission_success(self):
        self.create_test_user()
        account = Account.objects.create(id=1, name="")
        url = f"{self._get_url(account.id)}"
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_get_success(self):
        """ Test sync data is formatted correctly """

        self.create_test_user()
        account = Account.objects.create(id=next(int_iterator), name="")
        start = timezone.now().date()
        end = start + timedelta(days=1)
        campaign_breakout = CampaignCreation.objects \
            .create(account_creation=account.account_creation, budget=4, start=start, end=end, name="test campaign",
                    bid_strategy_type=CampaignCreation.MAX_CPV_STRATEGY, type=CampaignCreation.VIDEO_TYPE)
        ad_group_breakout = AdGroupCreation.objects.create(campaign_creation=campaign_breakout, max_rate=10,
                                                           name="test ad group")
        response = self.client.get(self._get_url(account.id))
        data = response.data
        campaign_data = data["campaigns"][0]
        ad_group_data = data["ad_groups"][0]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(data.keys()), {"campaigns", "ad_groups"})
        self.assertEqual(int(campaign_data["budget"]), int(campaign_breakout.budget))
        self.assertEqual(campaign_data["name"], campaign_breakout.name)
        self.assertEqual(campaign_data["bid_strategy_type"], campaign_breakout.bid_strategy_type)
        self.assertEqual(campaign_data["type"], campaign_breakout.type)

        self.assertEqual(int(ad_group_data["max_rate"]), int(ad_group_breakout.max_rate))
        self.assertEqual(ad_group_data["name"], ad_group_breakout.name)
        self.assertEqual(ad_group_data["campaign_name"], campaign_breakout.name)
        self.assertEqual(ad_group_data["campaign_type"], campaign_breakout.type)

    def test_patch_status_success(self):
        self.create_test_user()
        account = Account.objects.create(id=next(int_iterator), name="")
        start = timezone.now().date()
        end = start + timedelta(days=1)
        campaign_breakout = CampaignCreation.objects \
            .create(account_creation=account.account_creation, budget=4, start=start, end=end, name="test campaign",
                    bid_strategy_type=CampaignCreation.MAX_CPV_STRATEGY, type=CampaignCreation.VIDEO_TYPE)
        ad_group_breakout = AdGroupCreation.objects.create(campaign_creation=campaign_breakout, max_rate=10,
                                                           name="test ad group")
        payload = dict(
            campaign_ids=[campaign_breakout.id],
            ad_group_ids=[ad_group_breakout.id],
        )
        response = self.client.patch(self._get_url(account.id), data=json.dumps(payload),
                                     content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign_breakout.refresh_from_db()
        ad_group_breakout.refresh_from_db()
        self.assertTrue(campaign_breakout.sync_at >= campaign_breakout.updated_at)
        self.assertTrue(ad_group_breakout.sync_at >= ad_group_breakout.updated_at)

        to_update_campaign = CampaignCreation.objects.filter(
            Q(sync_at__lte=F("updated_at")) | Q(sync_at=None),
            account_creation=account.account_creation
        )
        to_update_ad_group = AdGroupCreation.objects.filter(
            Q(sync_at__lte=F("updated_at")) | Q(sync_at=None),
            campaign_creation__account_creation=account.account_creation
        )
        self.assertFalse(to_update_campaign.exists())
        self.assertFalse(to_update_ad_group.exists())
