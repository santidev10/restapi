import json
from unittest import mock

from oauth.constants import OAuthType
from oauth.models import Account
from oauth.models import AdGroup
from oauth.models import Campaign
from oauth.models import OAuthAccount
from segment.models import CustomSegment
from segment.models.constants import SegmentTypeEnum
from segment.utils.utils import get_gads_sync_code
from segment.utils.utils import GADS_ADGROUP_PLACEMENT_LIMIT
from segment.models.utils.segment_exporter import SegmentExporter
from segment.models import SegmentAdGroupSync
from utils.unittests.test_case import ExtendedAPITestCase


@mock.patch.object(SegmentExporter, "get_extract_export_ids", return_value=[])
class GadsSyncCodeTestCase(ExtendedAPITestCase):
    def _create_test_data(self, segment_type=SegmentTypeEnum.CHANNEL.value):
        gads_oauth_type = OAuthType.GOOGLE_ADS.value
        user = self.create_test_user()
        ctl = CustomSegment.objects.create(segment_type=segment_type)
        oauth_account = OAuthAccount.objects.create(oauth_type=gads_oauth_type, user=user)
        account = Account.objects.create()
        campaign = Campaign.objects.create(account=account, oauth_type=gads_oauth_type)
        adgroups = [AdGroup.objects.create(campaign=campaign, oauth_type=gads_oauth_type) for _ in range(2)]
        syncs = [SegmentAdGroupSync.objects.create(segment=ctl, adgroup=adgroups[i]) for i in range(2)]
        oauth_account.gads_accounts.add(account)
        return ctl, account, syncs

    def test_get_none_no_syncs(self, mock_get_ids):
        """ Test function returns None if there is nothing to sync """
        ctl, account, syncs = self._create_test_data()
        SegmentAdGroupSync.objects.filter(id__in=[s.id for s in syncs]).update(is_synced=True)
        code = get_gads_sync_code(account)
        self.assertIsNone(code)

    def test_code_contains_ids(self, mock_get_ids):
        """ Test function returns execution code with all placeholders replaced with resource ids """
        ctl, account, syncs = self._create_test_data()
        with self.subTest("Test all ids are replaced"):
            code = get_gads_sync_code(account)
            self.assertTrue(json.dumps([s.adgroup_id for s in syncs]) in code)
            self.assertTrue(str(ctl.id) in code)

        with self.subTest("Test excludes syncs with is_synced = True"):
            # Consider syncs[0].is_synced = False, syncs[1].is_synced = True
            SegmentAdGroupSync.objects.filter(id=syncs[1].id).update(is_synced=True)
            code = get_gads_sync_code(account)
            self.assertFalse(json.dumps([s.adgroup_id for s in syncs]) in code)
            self.assertTrue(json.dumps([syncs[0].adgroup_id]) in code)
            self.assertTrue(str(ctl.id) in code)

    def test_code_ctl_placeholders(self, mock_get_ids):
        """
        Test that placementType uses CustomSegment.segment_type string value (channel, video)
        as Google Ads Scripts methods contain those strings
        """
        with self.subTest("Test video"):
            ctl, account, syncs = self._create_test_data(segment_type=SegmentTypeEnum.VIDEO.value)
            code = get_gads_sync_code(account)
            self.assertTrue("Video" in code)
            self.assertFalse("Channel" in code)

        with self.subTest("Test video"):
            ctl, account, syncs = self._create_test_data()
            code = get_gads_sync_code(account)
            self.assertTrue("Channel" in code)
            self.assertFalse("Video" in code)

    def test_code_replaces_placement_ids(self,  mock_get_ids):
        with self.subTest("Test placement ids are replaced"):
            mock_ids = [f"test_id_{i}" for i in range(5)]
            mock_get_ids.return_value = mock_ids
            ctl, account, syncs = self._create_test_data(segment_type=SegmentTypeEnum.VIDEO.value)
            code = get_gads_sync_code(account)
            self.assertTrue(json.dumps(mock_ids) in code)

        with self.subTest("Test that placement ids limit is 20k as Google Ads only allows 20k inclusion placements"
                          "per Ad Group"):
            mock_ids = [f"test_id_{i}" for i in range(GADS_ADGROUP_PLACEMENT_LIMIT + 1)]
            mock_get_ids.return_value = mock_ids
            ctl, account, syncs = self._create_test_data(segment_type=SegmentTypeEnum.VIDEO.value)
            code = get_gads_sync_code(account)
            # Extract placement id sync data that is replaced into script code
            start = code.find(mock_ids[0]) - 1
            end = code.find('], "placementType"')
            sync_placement_ids = json.loads(f"[{code[start:end]}]")
            self.assertEqual(len(sync_placement_ids), GADS_ADGROUP_PLACEMENT_LIMIT)
