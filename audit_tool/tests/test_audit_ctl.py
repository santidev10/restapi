from io import BytesIO
from mock import patch

import boto3
from django.conf import settings
from django.utils import timezone
from moto import mock_s3

from audit_tool.management.commands.audit_video_meta import Command
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditChannel
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import AuditChannelProcessor
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models.utils.generate_segment_utils import GenerateSegmentUtils
from utils.unittests.test_case import ExtendedAPITestCase


class TestAuditCTL(ExtendedAPITestCase):
    multi_db = True

    @mock_s3
    def test_update_video_ctl_success(self):
        """ Test audit_video_meta.update_ctl method updates CTL successfully """
        user = self.create_admin_user()
        now = timezone.now()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        conn.create_bucket(Bucket=settings.AMAZON_S3_AUDITS_FILES_BUCKET_NAME)

        segment = CustomSegment.objects.create(owner=user, segment_type=0)
        audit = AuditProcessor.objects.create(audit_type=1, source=2, params=dict(segment_id=segment.id))
        # Create mock generated export for audit to filter
        mock_urls = [f"https://www.youtube.com/watch?v={i}" for i in range(10)]
        v_audit = [AuditVideo.objects.create(video_id=url.split("?v=")[-1]) for url in mock_urls]
        v_processors = [AuditVideoProcessor(audit=audit, clean=v.id % 2 == 0, processed=now, video=v) for v in v_audit]
        AuditVideoProcessor.objects.bulk_create(v_processors)

        mock_export = BytesIO()
        mock_export.name = "mock_export.csv"
        mock_export.write(b"mock_video_urls\n")
        mock_export.write("\n".join(mock_urls).encode("utf-8"))
        mock_export.seek(0)
        CustomSegmentFileUpload.objects.create(segment=segment, filename=mock_export.name, query={})
        conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, mock_export.name).put(Body=mock_export)

        unclean = [v for v in v_processors if v.clean is False]
        expected_clean = [v for v in v_processors if v.clean is True]
        audit_command = Command()
        audit_command.audit = audit
        mock_stats = {
            "clean_count": len(expected_clean)
        }
        with patch.object(GenerateSegmentUtils, "get_aggregations_by_ids", return_value=mock_stats):
            audit_command.update_ctl()
        segment.refresh_from_db()
        audit.refresh_from_db()
        updated_export = conn.Object(
            settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, segment.export.filename
        ).get()["Body"].read().decode("utf-8")
        self.assertTrue(all(f"https://www.youtube.com/watch?v={v.id}") in updated_export for v in expected_clean)
        self.assertTrue(all(f"https://www.youtube.com/watch?v={v.id}") in updated_export for v in unclean)
        self.assertEqual(segment.statistics["clean_count"], mock_stats["clean_count"])

    @mock_s3
    def test_update_channel_ctl_success(self):
        """ Test audit_video_meta.update_ctl method updates channel CTL successfully """
        user = self.create_admin_user()
        now = timezone.now()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        conn.create_bucket(Bucket=settings.AMAZON_S3_AUDITS_FILES_BUCKET_NAME)

        segment = CustomSegment.objects.create(owner=user, segment_type=1)
        audit = AuditProcessor.objects.create(audit_type=2, source=2, params=dict(segment_id=segment.id))
        # Create mock generated export for audit to filter
        mock_urls = [f"https://www.youtube.com/channel/{i}" for i in range(5)]
        c_audit = [AuditChannel.objects.create(channel_id=url.split("/channel/")[-1]) for url in mock_urls]
        c_processors = [AuditChannelProcessor(audit=audit, clean=c.id % 2 == 0, processed=now, channel=c) for c in c_audit]
        AuditChannelProcessor.objects.bulk_create(c_processors)

        mock_export = BytesIO()
        mock_export.name = "mock_export.csv"
        mock_export.write(b"mock_channel_urls\n")
        mock_export.write("\n".join(mock_urls).encode("utf-8"))
        mock_export.seek(0)
        CustomSegmentFileUpload.objects.create(segment=segment, filename=mock_export.name, query={})
        conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, mock_export.name).put(Body=mock_export)

        unclean = [c for c in c_processors if c.clean is False]
        expected_clean = [c for c in c_processors if c.clean is True]
        audit_command = Command()
        audit_command.audit = audit
        mock_stats = {
            "clean_channel_count": len(expected_clean)
        }
        with patch.object(GenerateSegmentUtils, "get_aggregations_by_ids", return_value=mock_stats):
            audit_command.update_ctl()
        segment.refresh_from_db()
        audit.refresh_from_db()
        updated_export = conn.Object(
            settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, segment.export.filename
        ).get()["Body"].read().decode("utf-8")
        self.assertTrue(all(f"https://www.youtube.com/channel/{c.id}") in updated_export for c in expected_clean)
        self.assertTrue(all(f"https://www.youtube.com/channel/{c.id}") in updated_export for c in unclean)
        self.assertEqual(segment.statistics["clean_channel_count"], mock_stats["clean_channel_count"])
