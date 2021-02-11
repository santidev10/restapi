from uuid import uuid4

from django.test import TestCase
from mock import patch

from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoVet
from audit_tool.tasks.generate_audit_items import generate_audit_items
from segment.models import CustomSegment
from segment.models.utils.segment_exporter import SegmentExporter
from utils.unittests.int_iterator import int_iterator
from utils.youtube_api import YoutubeAPIConnector
from utils.youtube_api import YoutubeAPIConnectorException


class GenerateAuditItemsTestCase(TestCase):
    databases = '__all__'

    def _bulk_create(self, model, objs, *argss, **kwargs):
        model.objects.bulk_create(objs)

    def test_handle_channel_youtube_connector_exception(self):
        segment = CustomSegment.objects.create(title="test_segment", segment_type=1, list_type=0, uuid=uuid4())
        audit_item_ids = [f"youtube_channel_id_{next(int_iterator)}" for _ in range(5)]
        # Simulate a missing audit_item id
        audit_items = [AuditChannel(channel_id=_id) for _id in audit_item_ids[1:]]
        AuditChannel.objects.bulk_create(audit_items)

        with patch.object(YoutubeAPIConnector, "_YoutubeAPIConnector__execute_call") as mock_connector, \
            patch.object(SegmentExporter, "get_extract_export_ids", return_value=audit_item_ids), \
            patch("audit_tool.segment_audit_generator.safe_bulk_create", side_effect=self._bulk_create):
            mock_connector.side_effect = YoutubeAPIConnectorException
            generate_audit_items.delay(segment.id, data_field="channel")

        created = AuditChannelVet.objects.filter(channel__channel_id__in=[item.channel_id for item in audit_items])
        self.assertEqual(created.count(), len(audit_items))

    def test_handle_video_youtube_connector_exception(self):
        segment = CustomSegment.objects.create(title="test_segment", segment_type=0, list_type=0, uuid=uuid4())
        audit_item_ids = [f"video_id_{next(int_iterator)}"[-11:] for _ in range(5)]
        # Simulate a missing audit_item id
        audit_items = [AuditVideo(video_id=_id) for _id in audit_item_ids[1:]]
        AuditVideo.objects.bulk_create(audit_items)

        with patch.object(YoutubeAPIConnector, "_YoutubeAPIConnector__execute_call") as mock_connector, \
            patch.object(SegmentExporter, "get_extract_export_ids", return_value=audit_item_ids), \
            patch("audit_tool.segment_audit_generator.safe_bulk_create", side_effect=self._bulk_create):
            mock_connector.side_effect = YoutubeAPIConnectorException
            generate_audit_items.delay(segment.id, data_field="video")

        created = AuditVideoVet.objects.filter(video__video_id__in=[item.video_id for item in audit_items])
        self.assertEqual(created.count(), len(audit_items))
