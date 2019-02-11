from django.test import testcases
from mock import patch

from audit_tool.models import ChannelAuditIgnore
from audit_tool.segmented_audit import SegmentedAudit
from segment.models import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher


class SegmentedAuditTestCase(testcases.TransactionTestCase):
    def test_ignores_manual_channel_on_add(self):
        connector = SingleDatabaseApiConnectorPatcher()
        channels = connector.get_channel_list()["items"]
        any_channel = channels[-1]
        ChannelAuditIgnore.objects.create(channel_id=any_channel["id"])

        with patch("audit_tool.segmented_audit.Connector", new=SingleDatabaseApiConnectorPatcher):
            audit = SegmentedAudit()
            audit.run()

        self.assertFalse(PersistentSegmentChannel.objects.filter(related__related_id=any_channel["id"]).exists())

    def test_ignores_manual_channel_on_remove(self):
        connector = SingleDatabaseApiConnectorPatcher()
        channels = connector.get_channel_list()["items"]
        any_channel = channels[0]
        any_channel_id = any_channel["id"]
        segment = PersistentSegmentChannel.objects.create(
            category=PersistentSegmentCategory.WHITELIST,
            title=PersistentSegmentTitles.CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
        )
        PersistentSegmentRelatedChannel.objects.create(
            related_id=any_channel_id,
            segment=segment,
        )
        ChannelAuditIgnore.objects.create(channel_id=any_channel_id)
        related_item = PersistentSegmentRelatedChannel.objects.filter(
            related_id=any_channel_id,
            segment=segment,
        )
        self.assertTrue(related_item.exists())

        def mark_as_blacklist(item):
            if item["id"] == any_channel_id:
                return PersistentSegmentCategory.BLACKLIST
            return PersistentSegmentCategory.WHITELIST

        with patch("audit_tool.segmented_audit.Connector", new=SingleDatabaseApiConnectorPatcher), \
             patch.object(SegmentedAudit, "_segment_category", side_effect=mark_as_blacklist):
            audit = SegmentedAudit()
            audit.run()

        self.assertTrue(related_item.exists())
