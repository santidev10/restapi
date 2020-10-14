from audit_tool.models import AuditProcessor
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from segment.models import CustomSegment
from segment.tasks import update_segment_statistics
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase
from utils.datetime import now_in_default_tz


class UpdateSegmentStatisticsTestCase(ExtendedAPITestCase, ESTestCase):
    @classmethod
    def setUpClass(cls, *args, **kwargs):
        super().setUpClass()
        cls.channel_manager = ChannelManager([Sections.TASK_US_DATA])
        cls.video_manager = VideoManager([Sections.TASK_US_DATA])

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_channel_ctl_update_success(self):
        """ Test channel statistics updates successfully """
        now = now_in_default_tz()
        user = self.create_admin_user()
        audit = AuditProcessor.objects.create(source=1)
        segment = CustomSegment.objects.create(title="test", owner=user, segment_type=1, audit_id=audit.id)
        audit_utils = segment.audit_utils
        a1 = audit_utils.model.objects.create(
            channel_id=f"channel_{next(int_iterator)}", channel_id_hash=next(int_iterator)
        )
        a2 = audit_utils.model.objects.create(
            channel_id=f"channel_{next(int_iterator)}", channel_id_hash=next(int_iterator)
        )
        audit_utils.vetting_model.objects.create(audit=audit, channel=a1, processed=now, clean=True)
        audit_utils.vetting_model.objects.create(audit=audit, channel=a2, processed=now, clean=False)
        doc1 = Channel(a1.channel_id)
        doc2 = Channel(a2.channel_id)
        doc2.populate_task_us_data(brand_safety=["exists"])
        self.channel_manager.upsert([doc1, doc2])
        update_segment_statistics()
        segment.refresh_from_db()
        statistics = segment.statistics
        self.assertEqual(statistics["unvetted_items_count"], 0)
        self.assertEqual(statistics["vetted_items_count"], 2)
        self.assertEqual(statistics["safe_items_count"], 1)
        self.assertEqual(statistics["unsafe_items_count"], 1)
        self.assertEqual(statistics["suitable_items_count"], 1)
        self.assertEqual(statistics["unsuitable_items_count"], 1)

    def test_video_ctl_update_success(self):
        """ Test video statistics updates successfully """
        now = now_in_default_tz()
        user = self.create_admin_user()
        audit = AuditProcessor.objects.create(source=1)
        segment = CustomSegment.objects.create(title="test", owner=user, segment_type=0, audit_id=audit.id)
        audit_utils = segment.audit_utils
        a1 = audit_utils.model.objects.create(
            video_id=f"v_{next(int_iterator)}", video_id_hash=next(int_iterator)
        )
        a2 = audit_utils.model.objects.create(
            video_id=f"v_{next(int_iterator)}", video_id_hash=next(int_iterator)
        )
        a3 = audit_utils.model.objects.create(
            video_id=f"v_{next(int_iterator)}", video_id_hash=next(int_iterator)
        )
        a4 = audit_utils.model.objects.create(
            video_id=f"v_{next(int_iterator)}", video_id_hash=next(int_iterator)
        )
        audit_utils.vetting_model.objects.create(audit=audit, video=a1, processed=now, clean=True)
        audit_utils.vetting_model.objects.create(audit=audit, video=a2, processed=now, clean=False)
        audit_utils.vetting_model.objects.create(audit=audit, video=a3, processed=now, clean=False)
        audit_utils.vetting_model.objects.create(audit=audit, video=a4) # Unvetted
        doc1 = Video(a1.video_id)
        doc2 = Video(a2.video_id)
        doc3 = Video(a2.video_id)
        doc2.populate_task_us_data(brand_safety=["exists"])
        doc3.populate_task_us_data(brand_safety=["exists"])
        self.video_manager.upsert([doc1, doc2, doc3])
        update_segment_statistics()
        segment.refresh_from_db()
        statistics = segment.statistics
        self.assertEqual(statistics["unvetted_items_count"], 1)
        self.assertEqual(statistics["vetted_items_count"], 3)
        self.assertEqual(statistics["safe_items_count"], 1)
        self.assertEqual(statistics["unsafe_items_count"], 2)
        self.assertEqual(statistics["suitable_items_count"], 1)
        self.assertEqual(statistics["unsuitable_items_count"], 2)
