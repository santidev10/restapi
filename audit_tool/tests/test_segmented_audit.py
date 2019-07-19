from django.test import testcases
from unittest import skip
from mock import patch

from audit_tool.models import ChannelAuditIgnore
from audit_tool.models import VideoAuditIgnore
from audit_tool.segmented_audit import SegmentedAudit
from segment.models import PersistentSegmentChannel
from segment.models import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher


@skip
# SegmentedAudit deprecateed
class SegmentedAuditTestCase(testcases.TransactionTestCase):
    def test_ignores_manual_channel_on_add(self):
        connector = SingleDatabaseApiConnectorPatcher()
        channels = connector.get_channel_list()["items"]
        any_channel = channels[-1]
        any_channel_id = any_channel["channel_id"]
        ChannelAuditIgnore.objects.create(id=any_channel_id)

        audit = SegmentedAudit()
        audit.run()

        self.assertFalse(PersistentSegmentChannel.objects.filter(related__related_id=any_channel_id).exists())

    def test_ignores_manual_channel_on_remove(self):
        connector = SingleDatabaseApiConnectorPatcher()
        channels = connector.get_channel_list()["items"]
        any_channel = channels[0]
        any_channel_id = any_channel["channel_id"]
        segment = PersistentSegmentChannel.objects.create(
            category=PersistentSegmentCategory.WHITELIST,
            title=PersistentSegmentTitles.CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
        )
        PersistentSegmentRelatedChannel.objects.create(
            related_id=any_channel_id,
            segment=segment,
        )
        ChannelAuditIgnore.objects.create(id=any_channel_id)
        related_item = PersistentSegmentRelatedChannel.objects.filter(
            related_id=any_channel_id,
            segment=segment,
        )
        self.assertTrue(related_item.exists())

        def mark_as_blacklist(item):
            if item["channel_id"] == any_channel_id:
                return PersistentSegmentCategory.BLACKLIST
            return PersistentSegmentCategory.WHITELIST

        with patch.object(SegmentedAudit, "_segment_category", side_effect=mark_as_blacklist):
            audit = SegmentedAudit()
            audit.run()

        self.assertTrue(related_item.exists())

    def test_ignores_manual_video_on_add(self):
        connector = SingleDatabaseApiConnectorPatcher()
        videos = connector.get_video_list()["items"]
        any_video = videos[-1]
        any_video_id = any_video["video_id"]
        VideoAuditIgnore.objects.create(id=any_video_id)

        audit = SegmentedAudit()
        audit.run()

        self.assertFalse(PersistentSegmentVideo.objects.filter(related__related_id=any_video_id).exists())

    def test_ignores_manual_video_on_remove(self):
        connector = SingleDatabaseApiConnectorPatcher()
        videos = connector.get_video_list()["items"]
        any_video = videos[0]
        any_video_id = any_video["video_id"]
        segment = PersistentSegmentVideo.objects.create(
            category=PersistentSegmentCategory.WHITELIST,
            title=PersistentSegmentTitles.VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE,
        )
        PersistentSegmentRelatedVideo.objects.create(
            related_id=any_video_id,
            segment=segment,
        )
        VideoAuditIgnore.objects.create(id=any_video_id)
        related_item = PersistentSegmentRelatedVideo.objects.filter(
            related_id=any_video_id,
            segment=segment,
        )
        self.assertTrue(related_item.exists())

        def mark_as_blacklist(item):
            if item.get("video_id") == any_video_id:
                return PersistentSegmentCategory.BLACKLIST
            return PersistentSegmentCategory.WHITELIST

        with patch.object(SegmentedAudit, "_segment_category", side_effect=mark_as_blacklist):
            audit = SegmentedAudit()
            audit.run()

        self.assertTrue(related_item.exists())
