import json

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND

from utils.unittests.test_case import ExtendedAPITestCase
from segment.api.urls.names import Name
from segment.models.constants import SegmentTypeEnum
from saas.urls.namespaces import Namespace
from segment.models import CustomSegment
from segment.models.constants import Params

from oauth.models import OAuthAccount
from oauth.constants import OAuthType
from oauth.models import Account
from ouath.models import Campaign
from oauth.models import AdGroup
from utils.unittests.int_iterator import int_iterator


class CTLOAuthTestCase(ExtendedAPITestCase):

    _url = reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_OAUTH)

    def _mock_data(self, oauth_account):
        account = Account.objects.create(oauth_account=oauth_account)
        campaign = Campaign.objects.create(account=account, oauth_type=oauth_account.oauth_type if oauth_account else None)
        ad_groups = [AdGroup.objects.create(campaign=campaign)]
        return account, campaign, ad_groups

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user()
        self.oauth_account = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.GOOGLE_ADS.value)

    def test_oauth_success(self):
        pass

    def test_permission_fail(self):
        pass

    def test_get_account_ids(self):
        """ Test successfully get Account ids with oauthed user """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        accounts = [Account.objects.create(id=next(int_iterator), oauth_account=self.oauth_account) for _ in range(2)]
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data, [a.id for a in accounts])

    def test_get_adgroups(self):
        """ Test successfully get AdGroup ids with oauthed user for Account id """
        account, campaign, ad_groups = self._mock_data(self.oauth_account)
        account2 , campaign2, _ = self._mock_data(None)
        response = self.client.get(self._url + f"?cid={account.id}")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, [ag.id for ag in ad_groups])
