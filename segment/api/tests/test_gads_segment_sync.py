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
from segment.models.utils.segment_exporter import SegmentExporter
from segment.models import SegmentAdGroupSync
from segment.models import CustomSegment
from segment.models.constants import Params
from segment.models.constants import Results
from segment.models.constants import SegmentTypeEnum
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.patch_bulk_create import patch_bulk_create


@mock.patch("segment.api.serializers.ctl_gads_sync_serializer.safe_bulk_create", patch_bulk_create)
class CTLGadsSyncTestCase(ExtendedAPITestCase):

    def _get_url(self, pk):
        url = reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_SYNC, kwargs=dict(pk=pk))
        return url

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user()
        self.oauth_account = OAuthAccount.objects.create(user=self.user, oauth_type=OAuthType.GOOGLE_ADS.value)

    def _mock_data(self, oauth_account=None):
        oauth_account = oauth_account or self.oauth_account
        account = Account.objects.create()
        campaign = Campaign.objects.create(account=account,
                                           oauth_type=oauth_account.oauth_type if oauth_account else None)
        adgroups = [AdGroup.objects.create(campaign=campaign, oauth_type=OAuthType.GOOGLE_ADS.value)]
        oauth_account.gads_accounts.add(account)
        return account, campaign, adgroups

    def test_get_sync_data(self):
        """ Test that data is retrieved using Google Ads account Account id as pk, as Google Ads is unaware of ViewIQ data """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        account, campaign, adgroups = self._mock_data()

        with self.subTest("No code if is_synced is True, as it has already been synced"):
            SegmentAdGroupSync.objects.bulk_create([
                SegmentAdGroupSync(adgroup=ag, segment=ctl, is_synced=True) for ag in adgroups
            ])
            response = self.client.get(self._get_url(account.id))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertIsNone(response.data["code"])

        with self.subTest("Execution code if is_synced is False, as it has been marked for sync"),\
                mock.patch.object(SegmentExporter, "get_extract_export_ids", return_value=[]):
            SegmentAdGroupSync.objects.filter(adgroup_id__in=[ag.id for ag in adgroups]).update(is_synced=False)
            response = self.client.get(self._get_url(account.id))
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertTrue(response.data["code"])

    def test_post_creates_sync_record(self):
        """ Test POST for first time creates sync record """
        segment = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        account, campaign, adgroups = self._mock_data()
        ag_ids = [ag.id for ag in adgroups]
        self.assertFalse(SegmentAdGroupSync.objects.filter(segment=segment, adgroup_id__in=ag_ids).exists())
        payload = json.dumps({
            Params.SEGMENT_ID: segment.id,
            Params.ADGROUP_IDS: ag_ids
        })
        response = self.client.post(self._get_url(account.id), data=payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        syncs = SegmentAdGroupSync.objects.filter(segment=segment)
        self.assertEqual([ag.adgroup_id for ag in syncs], ag_ids)
        # is_synced = False marks for pending update
        self.assertEqual(set(ag.is_synced for ag in syncs), {False})

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
        account, campaign, adgroups = self._mock_data(self.oauth_account)
        ag_ids = [ag.id for ag in adgroups]
        SegmentAdGroupSync.objects.bulk_create([
            SegmentAdGroupSync(adgroup=ag, segment=ctl, is_synced=True) for ag in adgroups
        ])
        payload = {
            Params.SEGMENT_ID: ctl.id,
            Params.ADGROUP_IDS: ag_ids,
        }
        response = self.client.post(self._get_url(account.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(all(v.is_synced is False for v in SegmentAdGroupSync.objects.filter(adgroup_id__in=ag_ids)))

    def test_post_change_ctl(self):
        """ Test updating Adgroup ctl target """
        prev_ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        account, campaign, adgroups = self._mock_data(self.oauth_account)
        SegmentAdGroupSync.objects.bulk_create([
            SegmentAdGroupSync(adgroup=ag, segment=prev_ctl, is_synced=False) for ag in adgroups
        ])

        new_ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        payload = {
            Params.SEGMENT_ID: new_ctl.id,
            Params.ADGROUP_IDS: [adgroups[0].id]
        }
        response = self.client.post(self._get_url(account.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        account.refresh_from_db()
        updated_sync = SegmentAdGroupSync.objects.get(adgroup_id=adgroups[0].id)
        self.assertEqual(updated_sync.segment.id, new_ctl.id)

    def test_patch_update_sync_history(self):
        """
        Test patch updates sync history.
        PK in url is Account id, since this request will be coming from Google Ads and only link between the two is the
        account id
        """
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        account, campaign, adgroups = self._mock_data(self.oauth_account)
        SegmentAdGroupSync.objects.bulk_create([
            SegmentAdGroupSync(adgroup=ag, segment=ctl, is_synced=True) for ag in adgroups
        ])
        payload = {
            "adgroup_ids": [ag.id for ag in adgroups]
        }
        response = self.client.patch(self._get_url(account.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        ctl.refresh_from_db()
        self.assertTrue(len(ctl.statistics[Results.GADS_SYNC_DATA][Results.HISTORY]) == 1)

    def test_patch_update_sync_status(self):
        """ Test that SegmentAdGroupSync.is_synced = True after successful sync """
        account, campaign, adgroups = self._mock_data(self.oauth_account)
        ag_ids = [ag.id for ag in adgroups]
        ctl = CustomSegment.objects.create(owner=self.user, segment_type=SegmentTypeEnum.CHANNEL.value)
        SegmentAdGroupSync.objects.bulk_create([
            SegmentAdGroupSync(adgroup=ag, segment=ctl, is_synced=False) for ag in adgroups
        ])
        payload = {
            "adgroup_ids": ag_ids
        }
        response = self.client.patch(self._get_url(account.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(all(sync.is_synced for sync in SegmentAdGroupSync.objects.filter(adgroup_id__in=ag_ids)))
