import json

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from utils.unittests.test_case import ExtendedAPITestCase
from segment.api.urls.names import Name
from segment.models.constants import SegmentTypeEnum
from saas.urls.namespaces import Namespace
from segment.models import CustomSegment
from segment.models.constants import Params

from oauth.constants import OAuthType
from oauth.models import OAuthAccount
from oauth.models import Account
from oauth.models import Campaign
from oauth.models import AdGroup
from utils.unittests.int_iterator import int_iterator


class CTLSyncTestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_SYNC)

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user()
        self.oauth_account = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.GOOGLE_ADS.value)

    def _mock_data(self, oauth_account):
        account = Account.objects.create(oauth_account=oauth_account)
        campaign = Campaign.objects.create(account=account,
                                           oauth_type=oauth_account.oauth_type if oauth_account else None)
        ad_groups = [AdGroup.objects.create(campaign=campaign)]
        return account, campaign, ad_groups

    def test_post(self):
        """ Test updating CTL with sync data and marks for update with Google Ads """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        with self.subTest("Must provide valid Account id"):
            payload = {
                "pk": ctl.id,
                Params.GoogleAds.CID: 1,
                Params.GoogleAds.AD_GROUP_IDS: next(int_iterator),
            }
            response = self.client.patch(self._url, data=json.dumps(payload), content_type="application/json")
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

        with self.subTest("Must provide valid Adgroup Id's"):
            payload = {
                "pk": ctl.id,
                Params.GoogleAds.CID: next(int_iterator),
                Params.GoogleAds.AD_GROUP_IDS: [],
            }
            response = self.client.patch(self._url, data=json.dumps(payload), content_type="application/json")
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

        with self.subTest("Test success"):
            account, campaign, ad_groups = self._mock_data(self.oauth_account)
            payload = {
                "pk": ctl.id,
                Params.GoogleAds.CID: account.id,
                Params.GoogleAds.AD_GROUP_IDS: [ag.id for ag in ad_groups],
            }
            response = self.client.patch(self._url, data=json.dumps(payload), content_type="application/json")
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_update_sync_history(self):
        """ Test patch updates sync history """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
