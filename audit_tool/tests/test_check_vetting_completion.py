from django.utils import timezone
import uuid

from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoVet
from audit_tool.tasks.check_vetting_completion import check_vetting_completion
from segment.models import CustomSegment
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.int_iterator import int_iterator


class CheckVettingCompletion(ExtendedAPITestCase):
    def test_video_completion_success(self):
        before = timezone.now()
        audit = AuditProcessor.objects.create(source=1, audit_type=1)
        segment = CustomSegment.objects.create(audit_id=audit.id, uuid=uuid.uuid4(), is_vetting_complete=False,
                                               title="", list_type=0, segment_type=0)
        vetting_items = []
        for i in range(3):
            audit_item = AuditVideo.objects.create(video_id=f"video_id{next(int_iterator)}")
            vetting_items.append(AuditVideoVet(audit=audit, video=audit_item, processed=before))
            check_vetting_completion()
        audit.refresh_from_db()
        self.assertEqual(segment.is_vetting_complete, True)
        self.assertTrue(audit.completed > before)
        
    def test_channel_completion_success(self):
        before = timezone.now()
        audit = AuditProcessor.objects.create(source=1, audit_type=2, completed=None)
        segment = CustomSegment.objects.create(audit_id=audit.id, uuid=uuid.uuid4(), is_vetting_complete=False,
                                               title="", list_type=0, segment_type=0)
        vetting_items = []
        for i in range(3):
            audit_item = AuditChannel.objects.create(channel_id=f"channel_id{next(int_iterator)}")
            vetting_items.append(AuditChannelVet(audit=audit, channel=audit_item, processed=before))
            check_vetting_completion()
        audit.refresh_from_db()
        self.assertEqual(segment.is_vetting_complete, True)
        self.assertTrue(audit.completed > before)
