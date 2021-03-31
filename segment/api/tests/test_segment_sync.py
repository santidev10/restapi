import json

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from oauth.constants import OAuthType
from oauth.models import Account
from oauth.models import AdGroup
from oauth.models import Campaign
from oauth.models import OAuthAccount
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models.constants import Params
from segment.models.constants import Results
from segment.models.constants import SegmentTypeEnum
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class CTLSyncTestCase(ExtendedAPITestCase):

    def _get_url(self, pk):
        url = reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_SYNC, kwargs=dict(pk=pk))
        return url

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user()
        self.oauth_account = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.GOOGLE_ADS.value)

    def _mock_data(self, oauth_account):
        account = Account.objects.create()
        campaign = Campaign.objects.create(account=account,
                                           oauth_type=oauth_account.oauth_type if oauth_account else None)
        ad_groups = [AdGroup.objects.create(campaign=campaign, oauth_type=OAuthType.GOOGLE_ADS.value)]
        oauth_account.gads_accounts.add(account)
        return account, campaign, ad_groups

    def test_post_update_for_sync(self):
        """ Test updating CTL with sync data and marks for update with Google Ads """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        with self.subTest("Must provide valid GAds Account id"):
            payload = {
                Params.GoogleAds.CID: 1,
                Params.GoogleAds.AD_GROUP_IDS: next(int_iterator),
            }
            response = self.client.post(self._get_url(ctl.id), data=json.dumps(payload), content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        with self.subTest("Must provide valid Adgroup Id's"):
            payload = {
                Params.GoogleAds.CID: next(int_iterator),
                Params.GoogleAds.AD_GROUP_IDS: [],
            }
            response = self.client.post(self._get_url(ctl.id), data=json.dumps(payload), content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        with self.subTest("Test success"):
            account, campaign, ad_groups = self._mock_data(self.oauth_account)
            payload = {
                "pk": ctl.id,
                Params.GoogleAds.CID: account.id,
                Params.GoogleAds.AD_GROUP_IDS: [ag.id for ag in ad_groups],
            }
            response = self.client.post(self._get_url(ctl.id), data=json.dumps(payload), content_type="application/json")
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_update_sync_history(self):
        """ Test patch updates sync history """
        account = Account.objects.create(name="Test Gads Account")
        params = {
            Params.GoogleAds.GADS_SYNC_DATA: {
                Params.GoogleAds.CID: account.id
            }
        }
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value, params=params)
        response = self.client.patch(self._get_url(ctl.id), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        ctl.refresh_from_db()
        self.assertTrue(len(ctl.statistics[Results.GADS][Results.HISTORY]) > 1)
