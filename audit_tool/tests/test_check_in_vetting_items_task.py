from datetime import timedelta

from django.utils import timezone

from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoVet
from audit_tool.tasks.check_in_vetting_items import check_in_vetting_items
from audit_tool.tasks.check_in_vetting_items import CHECKOUT_THRESHOLD
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.int_iterator import int_iterator


class CheckInVettingItemsTaskTestCase(ExtendedAPITestCase):
    def test_check_in_threshold(self):
        before = timezone.now() - timedelta(minutes=CHECKOUT_THRESHOLD + 5)
        after = timezone.now()
        video_audit = AuditProcessor.objects.create(source=1, audit_type=1)
        channel_audit = AuditProcessor.objects.create(source=1, audit_type=2)
        video_vets = []
        channel_vets = []
        for i in range(3):
            audit_item = AuditVideo.objects.create(video_id=f"video_id{next(int_iterator)}")
            video_vets.append(AuditVideoVet(audit=video_audit, video=audit_item, checked_out_at=before))
        for i in range(3):
            audit_item = AuditChannel.objects.create(channel_id=f"channel_id{next(int_iterator)}")
            channel_vets.append(AuditChannelVet(audit=channel_audit, channel=audit_item, checked_out_at=after))
        AuditVideoVet.objects.bulk_create(video_vets)
        AuditChannelVet.objects.bulk_create(channel_vets)

        self.assertTrue(all(item.checked_out_at is not None for item in video_vets))
        self.assertTrue(all(item.checked_out_at is not None for item in channel_vets))

        check_in_vetting_items()

        self.assertTrue(all(item.checked_out_at is None for item in AuditVideoVet.objects.all()))
        self.assertTrue(all(item.checked_out_at is not None for item in AuditChannelVet.objects.all()))

    def test_check_in_all_vetting_items(self):
        before = timezone.now() - timedelta(minutes=CHECKOUT_THRESHOLD + 10)
        video_audit = AuditProcessor.objects.create(source=1, audit_type=1)
        channel_audit = AuditProcessor.objects.create(source=1, audit_type=2)
        video_vets = []
        channel_vets = []
        for i in range(3):
            audit_item = AuditVideo.objects.create(video_id=f"video_id{next(int_iterator)}")
            video_vets.append(AuditVideoVet(audit=video_audit, video=audit_item, checked_out_at=before))
        for i in range(3):
            audit_item = AuditChannel.objects.create(channel_id=f"channel_id{next(int_iterator)}")
            channel_vets.append(AuditChannelVet(audit=channel_audit, channel=audit_item, checked_out_at=before))
        AuditVideoVet.objects.bulk_create(video_vets)
        AuditChannelVet.objects.bulk_create(channel_vets)

        self.assertTrue(all(item.checked_out_at is not None for item in video_vets))
        self.assertTrue(all(item.checked_out_at is not None for item in channel_vets))

        check_in_vetting_items()

        self.assertTrue(all(item.checked_out_at is None for item in AuditVideoVet.objects.all()))
        self.assertTrue(all(item.checked_out_at is None for item in AuditChannelVet.objects.all()))
