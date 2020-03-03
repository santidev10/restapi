from mock import patch

from django.utils import timezone
import uuid

from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoVet
from audit_tool.tasks.check_vetting_completion import check_vetting_completion
from segment.models import CustomSegment
from segment.models import CustomSegmentVettedFileUpload
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


def mock_generate(segment):
    CustomSegmentVettedFileUpload.objects.create(segment=segment)


@patch("audit_tool.tasks.check_vetting_completion.generate_vetted_segment")
class CheckVettingCompletion(ExtendedAPITestCase):
    def test_video_completion_success(self, mock_generate_vetted):
        before = timezone.now()
        audit = AuditProcessor.objects.create(source=1, audit_type=1, completed=before)
        segment = CustomSegment.objects.create(audit_id=audit.id, uuid=uuid.uuid4(), is_vetting_complete=False,
                                               title="", list_type=0, segment_type=0)
        vetting_items = []
        for i in range(3):
            audit_item = AuditVideo.objects.create(video_id=f"video_id{next(int_iterator)}")
            vetting_items.append(AuditVideoVet(audit=audit, video=audit_item, processed=before))
        AuditVideoVet.objects.bulk_create(vetting_items)

        mock_generate_vetted.side_effect = mock_generate
        mock_generate_vetted(segment)

        check_vetting_completion()
        audit.refresh_from_db()
        segment.refresh_from_db()
        self.assertEqual(segment.is_vetting_complete, True)
        self.assertTrue(audit.completed > before)
        self.assertTrue(hasattr(segment, "vetted_export"))
        mock_generate_vetted.assert_called_once()
        
    def test_channel_completion_success(self, mock_generate_vetted):
        before = timezone.now()
        audit = AuditProcessor.objects.create(source=1, audit_type=2, completed=before)
        segment = CustomSegment.objects.create(audit_id=audit.id, uuid=uuid.uuid4(), is_vetting_complete=False,
                                               title="", list_type=0, segment_type=0)
        vetting_items = []
        for i in range(3):
            audit_item = AuditChannel.objects.create(channel_id=f"channel_id{next(int_iterator)}")
            vetting_items.append(AuditChannelVet(audit=audit, channel=audit_item, processed=before))
        AuditChannelVet.objects.bulk_create(vetting_items)

        mock_generate_vetted.side_effect = mock_generate
        mock_generate_vetted(segment)

        check_vetting_completion()
        audit.refresh_from_db()
        segment.refresh_from_db()
        self.assertEqual(segment.is_vetting_complete, True)
        self.assertTrue(audit.completed > before)
        self.assertTrue(hasattr(segment, "vetted_export"))
        mock_generate_vetted.assert_called_once()

    def test_channel_ignore(self, mock_generate_vetted):
        """ Should not generate for incomplete lists """
        before = timezone.now()
        audit = AuditProcessor.objects.create(source=1, audit_type=2, completed=before)
        segment = CustomSegment.objects.create(audit_id=audit.id, uuid=uuid.uuid4(), is_vetting_complete=False,
                                               title="", list_type=0, segment_type=1)
        audit_item = AuditChannel.objects.create(channel_id=f"channel_id{next(int_iterator)}")
        AuditChannelVet.objects.create(audit=audit, channel=audit_item, processed=None)
        check_vetting_completion()
        audit.refresh_from_db()
        segment.refresh_from_db()
        self.assertEqual(segment.is_vetting_complete, False)
        self.assertEqual(mock_generate_vetted.call_count, 0)

    def test_video_to_ignore(self, mock_generate_vetted):
        """ Should not generate for incomplete lists """
        before = timezone.now()
        audit = AuditProcessor.objects.create(source=1, audit_type=1, completed=before)
        segment = CustomSegment.objects.create(audit_id=audit.id, uuid=uuid.uuid4(), is_vetting_complete=True,
                                               title="", list_type=0, segment_type=0)
        audit_item = AuditVideo.objects.create(video_id=f"v_id{next(int_iterator)}")
        AuditVideoVet.objects.create(audit=audit, video=audit_item, processed=None)
        check_vetting_completion()
        audit.refresh_from_db()
        segment.refresh_from_db()
        self.assertEqual(segment.is_vetting_complete, False)
        self.assertEqual(mock_generate_vetted.call_count, 0)
