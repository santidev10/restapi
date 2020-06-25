import boto3
from django.conf import settings
from django.utils import timezone
from moto import mock_s3

from audit_tool.models import AuditProcessor
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from segment.api.serializers.custom_segment_vetted_export_serializers import CustomSegmentChannelVettedExportSerializer
from segment.api.serializers.custom_segment_vetted_export_serializers import CustomSegmentVideoVettedExportSerializer
from segment.models import CustomSegment
from segment.tasks.generate_vetted_segment import generate_vetted_segment
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class GenerateVettedSegmentTestCase(ExtendedAPITestCase, ESTestCase):
    def setUp(self):
        sections = [Sections.GENERAL_DATA]
        self.video_manager = VideoManager(sections=sections)
        self.channel_manager = ChannelManager(sections=sections)

    @mock_s3
    def test_generate_channel_vetted_headers(self):
        user = self.create_admin_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        audit = AuditProcessor.objects.create(source=1)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, owner=user, audit_id=audit.id,
        )
        ids = [f"channel_{next(int_iterator)}" for _ in range(5)]
        now = timezone.now()
        for _id in ids:
            audit_item = segment.audit_utils.model.objects.create(channel_id=_id)
            segment.audit_utils.vetting_model.objects.create(channel=audit_item, audit=audit, processed=now)
        self.channel_manager.upsert([self.channel_manager.model(_id) for _id in ids])
        generate_vetted_segment(segment.id)
        export_key = segment.get_vetted_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        header = [row.decode("utf-8") for row in body][0]
        self.assertTrue(set(header), CustomSegmentChannelVettedExportSerializer.columns)

    @mock_s3
    def test_generate_video_vetted_headers(self):
        user = self.create_admin_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        audit = AuditProcessor.objects.create(source=1)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=0, owner=user, audit_id=audit.id,
        )
        ids = [f"video_{next(int_iterator)}" for _ in range(5)]
        now = timezone.now()
        for _id in ids:
            audit_item = segment.audit_utils.model.objects.create(video_id=_id)
            segment.audit_utils.vetting_model.objects.create(video=audit_item, audit=audit, processed=now)
        self.video_manager.upsert([self.video_manager.model(_id) for _id in ids])
        generate_vetted_segment(segment.id)
        export_key = segment.get_vetted_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        header = [row.decode("utf-8") for row in body][0]
        self.assertTrue(set(header), CustomSegmentVideoVettedExportSerializer.columns)
