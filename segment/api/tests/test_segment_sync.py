import json
from unittest import mock

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
from segment.models import SegmentSync
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

    def test_get_sync_data(self):
        """ Test that data is retrieved using Google Ads account Account id as pk, as Google Ads is unaware of ViewIQ data """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        account = Account.objects.create(name="test_account")

        with self.subTest("No code if is_synced is None, as it has not been marked for sync"):
            SegmentSync.objects.update_or_create(segment=ctl, account=account, defaults=dict(is_synced=None))
            response = self.client.get(self._get_url(account.id))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertIsNone(response.data["code"])

        with self.subTest("No code if is_synced is True, as it has already been synced"):
            SegmentSync.objects.update_or_create(segment=ctl, account=account, defaults=dict(is_synced=True))
            response = self.client.get(self._get_url(account.id))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertIsNone(response.data["code"])

        with self.subTest("Execution code if is_synced is False, as it has been marked for sync"),\
                mock.patch("segment.api.views.custom_segment.segment_sync.SegmentSyncAPIView._get_code", return_value="test_code"):
                # mock.patch.object(SegmentExporter, "get_extract_export_ids", return_value=[]):
            SegmentSync.objects.update_or_create(segment=ctl, account=account, defaults=dict(is_synced=False))
            response = self.client.get(self._get_url(account.id))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertTrue(response.data["code"])

    def test_post_creates_sync_record(self):
        """ Test POST for first time creates sync record """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        account = Account.objects.create(name="test_account")
        self.assertFalse(SegmentSync.objects.filter(segment=ctl, account=account).exists())
        self.oauth_account.gads_accounts.add(account)
        ag_ids = [next(int_iterator)]
        payload = json.dumps({
            Params.SEGMENT_ID: ctl.id,
            Params.ADGROUP_IDS: ag_ids
        })
        response = self.client.post(self._get_url(account.id), data=payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        sync = SegmentSync.objects.get(segment=ctl, account=account)
        self.assertEqual(sync.data[Params.ADGROUP_IDS], ag_ids)
        # is_synced = False marks for pending update
        self.assertFalse(sync.is_synced)

    def test_post_fail(self):
        """ Test post fails with invalid data """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        with self.subTest("Must provide valid GAds Account id"):
            payload = {
                Params.CID: 1,
                Params.ADGROUP_IDS: next(int_iterator),
            }
            response = self.client.post(self._get_url(ctl.id), data=json.dumps(payload),
                                        content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        with self.subTest("Must provide valid Adgroup Id's"):
            payload = {
                Params.CID: next(int_iterator),
                Params.ADGROUP_IDS: [],
            }
            response = self.client.post(self._get_url(ctl.id), data=json.dumps(payload),
                                        content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_post_update_for_sync(self):
        """ Test updating CTL with sync data and marks for update with Google Ads """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        account, campaign, ad_groups = self._mock_data(self.oauth_account)
        ctl_sync = SegmentSync.objects.create(segment=ctl, account=account, is_synced=None)
        payload = {
            Params.SEGMENT_ID: ctl.id,
            Params.ADGROUP_IDS: [ag.id for ag in ad_groups],
        }
        response = self.client.post(self._get_url(account.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)

        ctl_sync.refresh_from_db()
        self.assertFalse(ctl_sync.is_synced)

    def test_post_change_ctl(self):
        """ Test updating Account sync target account """
        prev_ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        account, campaign, adgroups = self._mock_data(self.oauth_account)
        SegmentSync.objects.create(segment=prev_ctl, account=account)

        new_ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        payload = {
            Params.SEGMENT_ID: new_ctl.id,
            Params.ADGROUP_IDS: [adgroups[0].id]
        }
        response = self.client.post(self._get_url(account.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        account.refresh_from_db()
        self.assertNotEqual(account.sync.segment.id, prev_ctl.id)
        self.assertEqual(account.sync.segment.id, new_ctl.id)

    def test_patch_update_sync_history(self):
        """
        Test patch updates sync history.
        PK in url is Account id, since this request will be coming from Google Ads and only link between the two is the
        account id
        """
        account = Account.objects.create(name="Test Gads Account")
        params = {
            Params.GADS_SYNC_DATA: {
                Params.CID: account.id
            }
        }
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value, params=params)
        sync = SegmentSync.objects.create(segment=ctl, account=account)
        response = self.client.patch(self._get_url(account.id), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        ctl.refresh_from_db()
        sync.refresh_from_db()
        self.assertTrue(len(ctl.statistics[Results.GADS_SYNC_DATA][Results.HISTORY]) == 1)
        self.assertEqual(sync.is_synced, True)

    def test_patch_update_sync_status(self):
        """ Test that SegmentSync.is_synced = True after successful sync """
        account = Account.objects.create(name="Test Gads Account")
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        sync = SegmentSync.objects.create(segment=ctl, account=account)
        response = self.client.patch(self._get_url(account.id), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        sync.refresh_from_db()
        self.assertEqual(sync.is_synced, True)
