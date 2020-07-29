import boto3
from django.conf import settings
from django.utils import timezone
import io
from moto import mock_s3

from audit_tool.models import AuditProcessor
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from segment.api.export_serializers import CustomSegmentChannelVettedExportSerializer
from segment.api.export_serializers import CustomSegmentVideoVettedExportSerializer
from segment.models import CustomSegment
from segment.models import CustomSegmentSourceFileUpload
from segment.models import CustomSegmentVettedFileUpload
from segment.models.constants import SourceListType
from segment.tasks.generate_vetted_segment import generate_vetted_segment
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class GenerateVettedSegmentTestCase(ExtendedAPITestCase, ESTestCase):
    def setUp(self):
        sections = [Sections.GENERAL_DATA, Sections.TASK_US_DATA]
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

    def test_s3_key_retrieval(self):
        """
        test that segment.get_vetted_s3_key retrieves existing s3 key if available
        """
        user = self.create_admin_user()
        audit = AuditProcessor.objects.create(source=1)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, owner=user, audit_id=audit.id,
        )
        old_s3_key = segment.get_vetted_s3_key()
        new_s3_key = "new_s3_key.csv"
        CustomSegmentVettedFileUpload.objects.create(
            segment=segment,
            download_url='some-download-url.com/some/dir/file.csv',
            filename=new_s3_key
        )
        self.assertEqual(new_s3_key, segment.get_vetted_s3_key())
        self.assertNotEqual(old_s3_key, new_s3_key)

    @mock_s3
    def test_vetted_channel_export_source_list_10k(self):
        """ Test that vetted channel exports with vetted lists <= 10k has vetted data """
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        # Prepare docs to build segment
        _id = next(int_iterator)
        doc = ChannelManager.model(f"channel_id_{_id}")
        doc.populate_general_data(title=f"channel_title_{_id}")
        doc.populate_task_us_data(
            age_group=1,
            brand_safety=[1],
            gender=1,
            iab_categories=["Test"]
        )
        # Prepare inclusion source list of urls
        source_file = io.BytesIO()
        source_key = f"source_{next(int_iterator)}"
        inclusion_urls = [f"https://www.youtube.com/channel/{doc.main.id}".encode("utf-8")]
        source_file.write(b"URL\n")
        source_file.write(b"\n".join(inclusion_urls))
        source_file.seek(0)
        conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, source_key).put(Body=source_file)
        self.channel_manager.upsert([doc])
        audit = AuditProcessor.objects.create(source=1)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, list_type=0, audit_id=audit.id,
        )
        CustomSegmentVettedFileUpload.objects.create(
            segment=segment,
            download_url='some-download-url.com/some/dir/file.csv',
            filename=source_key
        )
        CustomSegmentSourceFileUpload.objects.create(
            segment=segment, source_type=SourceListType.INCLUSION.value, filename=source_key,
        )
        audit_item = segment.audit_utils.model.objects.create(channel_id=doc.main.id)
        segment.audit_utils.vetting_model.objects.create(channel=audit_item, audit=audit, processed=timezone.now())
        generate_vetted_segment(segment.id)
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, source_key).get()["Body"]
        rows = ",".join([row.decode("utf-8") for row in body])
        self.assertTrue(doc.main.id in rows)
        self.assertTrue("Y" in rows)
        self.assertTrue("4 - 8 Young Kids" in rows)
        self.assertTrue("Female" in rows)

    @mock_s3
    def test_vetted_video_export_source_list_10k(self):
        """ Test that vetted video exports with vetted lists <= 10k has vetted data """
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        # Prepare docs to build segment
        _id = next(int_iterator)
        doc = VideoManager.model(f"video_id_{_id}")
        doc.populate_general_data(title=f"video_title_{_id}")
        doc.populate_task_us_data(
            age_group=0,
            brand_safety=[0],
            gender=0,
            iab_categories=["Test"]
        )
        # Prepare inclusion source list of urls
        source_file = io.BytesIO()
        source_key = f"source_{next(int_iterator)}"
        inclusion_urls = [f"https://www.youtube.com/watch?v={doc.main.id}".encode("utf-8")]
        source_file.write(b"URL\n")
        source_file.write(b"\n".join(inclusion_urls))
        source_file.seek(0)
        conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, source_key).put(Body=source_file)
        self.video_manager.upsert([doc])
        audit = AuditProcessor.objects.create(source=1)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=0, list_type=0, audit_id=audit.id,
        )
        CustomSegmentVettedFileUpload.objects.create(
            segment=segment,
            download_url='some-download-url.com/some/dir/file.csv',
            filename=source_key
        )
        CustomSegmentSourceFileUpload.objects.create(
            segment=segment, source_type=SourceListType.INCLUSION.value, filename=source_key,
        )
        audit_item = segment.audit_utils.model.objects.create(video_id=doc.main.id)
        segment.audit_utils.vetting_model.objects.create(video=audit_item, audit=audit, processed=timezone.now())
        generate_vetted_segment(segment.id)
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, source_key).get()["Body"]
        rows = ",".join([row.decode("utf-8") for row in body])
        self.assertTrue(doc.main.id in rows)
        self.assertTrue("Y" in rows)
        self.assertTrue("0 - 3 Toddlers" in rows)
        self.assertTrue("Neutral" in rows)
