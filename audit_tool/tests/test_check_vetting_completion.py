from django.utils import timezone

from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoVet
from audit_tool.tasks.check_vetting_completion import check_vetting_completion
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.int_iterator import int_iterator


class CheckVettingCompletion(ExtendedAPITestCase):
    def test_video_completion_success(self):
        before = timezone.now()
        audit = AuditProcessor.objects.create(source=1, audit_type=1, completed=None)
        vetting_items = []
        for i in range(3):
            audit_item = AuditVideo.objects.create(video_id=f"video_id{next(int_iterator)}")
            vetting_items.append(AuditVideoVet(audit=audit, video=audit_item, processed=before))
        check_vetting_completion.run()
        audit.refresh_from_db()
        self.assertIsNotNone(audit.completed)
        self.assertTrue(audit.completed > before)
        
    def test_channel_completion_success(self):
        before = timezone.now()
        audit = AuditProcessor.objects.create(source=1, audit_type=2, completed=None)
        vetting_items = []
        for i in range(3):
            audit_item = AuditChannel.objects.create(channel_id=f"channel_id{next(int_iterator)}")
            vetting_items.append(AuditChannelVet(audit=audit, channel=audit_item, processed=before))
        check_vetting_completion.run()
        audit.refresh_from_db()
        self.assertIsNotNone(audit.completed)
        self.assertTrue(audit.completed > before)
