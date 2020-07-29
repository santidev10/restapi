import io
from uuid import uuid4
from mock import patch

import boto3
from django.conf import settings
from django.utils import timezone
from elasticsearch_dsl import Q
from moto import mock_s3

from audit_tool.models import AuditContentType
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditGender
from audit_tool.models import AuditAgeGroup
from brand_safety.models import BadWordCategory
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from segment.api.export_serializers import CustomSegmentChannelExportSerializer
from segment.api.export_serializers import \
    CustomSegmentChannelWithMonetizationExportSerializer
from segment.api.export_serializers import CustomSegmentVideoExportSerializer
from segment.models import CustomSegment
from segment.models.constants import SourceListType
from segment.models.custom_segment_file_upload import CustomSegmentSourceFileUpload
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.tasks.generate_segment import generate_segment
from utils.brand_safety import map_brand_safety_score
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class GenerateSegmentTestCase(ExtendedAPITestCase, ESTestCase):
    def setUp(self):
        sections = [Sections.GENERAL_DATA, Sections.STATS, Sections.TASK_US_DATA, Sections.BRAND_SAFETY]
        self.video_manager = VideoManager(sections=sections)
        self.channel_manager = ChannelManager(sections=sections)
        BadWordCategory.objects.create(name="test")
        AuditContentType.objects.create(id=0, content_type=AuditContentType.to_str[0])
        AuditContentQuality.objects.create(id=0, quality=AuditContentQuality.to_str[0])
        AuditGender.objects.create(id=0, gender=AuditGender.to_str[0])
        AuditAgeGroup.objects.create(id=0, age_group=AuditAgeGroup.to_str[0])

    @mock_s3
    def test_generate_channel_monetized_headers(self):
        user = self.create_admin_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, owner=user,
        )
        self.channel_manager.upsert([self.channel_manager.model(f"channel_{next(int_iterator)}")])
        generate_segment(segment, Q(), 1)
        export_key = segment.get_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        header = [row.decode("utf-8") for row in body][0]
        self.assertTrue(set(header), CustomSegmentChannelWithMonetizationExportSerializer.columns)

    @mock_s3
    def test_generate_channel_non_monetized_headers(self):
        user = self.create_test_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, owner=user,
        )
        self.channel_manager.upsert([self.channel_manager.model(f"channel_{next(int_iterator)}")])
        generate_segment(segment, Q(), 1)
        export_key = segment.get_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        header = [row.decode("utf-8") for row in body][0]
        self.assertTrue(set(header), CustomSegmentChannelExportSerializer.columns)

    @mock_s3
    def test_generate_video_headers(self):
        user = self.create_admin_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=0, owner=user,
        )
        self.video_manager.upsert([self.video_manager.model(f"video_{next(int_iterator)}")])
        generate_segment(segment, Q(), 1)
        export_key = segment.get_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        header = [row.decode("utf-8") for row in body][0]
        self.assertTrue(set(header), CustomSegmentVideoExportSerializer.columns)

    @mock_s3
    def test_generate_without_source(self):
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        # Prepare docs to build segment
        docs = []
        for _ in range(5):
            _id = next(int_iterator)
            doc = VideoManager.model(f"id_{_id}")
            doc.populate_general_data(title=f"title_{_id}", age_restricted=False)
            docs.append(doc)
        self.video_manager.upsert(docs)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=0, uuid=uuid4(), list_type=0
        )
        generate_segment(segment, Q(), len(docs))
        export_key = segment.get_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        rows = ",".join([row.decode("utf-8") for row in body])
        self.assertTrue(rows)
        self.video_manager.delete([doc.main.id for doc in docs])

    @mock_s3
    def test_generate_filename(self):
        """" Test export filename should be segment title """
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        # Prepare docs to build segment
        docs = []
        for _ in range(5):
            _id = next(int_iterator)
            doc = VideoManager.model(f"id_{_id}")
            doc.populate_general_data(title=f"title_{_id}", age_restricted=False)
            docs.append(doc)
        self.video_manager.upsert(docs)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=0, uuid=uuid4(), list_type=0
        )
        generate_segment(segment, Q(), len(docs))
        export_key = segment.get_s3_key()
        file = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()
        self.assertIn(segment.title, file["ContentDisposition"])

    @mock_s3
    def test_generate_video_export_data(self):
        """ Test video export contains all data """
        bs_category = BadWordCategory.objects.first()
        content_type = AuditContentType.objects.first()
        content_quality = AuditContentQuality.objects.first()
        age_group = AuditAgeGroup.objects.first()
        gender = AuditGender.objects.first()

        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        _id = next(int_iterator)
        doc = VideoManager.model(f"id_{_id}")
        doc.populate_general_data(
            title=f"title_{_id}", age_restricted=False, lang_code="en", iab_categories=["Test"],
            country_code="US",
        )
        doc.populate_stats(views=99)
        doc.populate_brand_safety(overall_score=55)
        doc.populate_task_us_data(
            brand_safety=[bs_category.id],
            age_group=age_group.id,
            gender=gender.id,
            content_type=content_type.id,
            content_quality=content_quality.id,
            last_vetted_at=timezone.now(),
            mismatched_language=True,
        )
        docs = [doc]
        self.video_manager.upsert(docs)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=0, uuid=uuid4(), list_type=0
        )
        generate_segment(segment, Q(), len(docs))
        export_key = segment.get_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        rows = ",".join([row.decode("utf-8") for row in body])

        self.assertIn(doc.general_data.title, rows)
        self.assertIn(doc.general_data.country_code, rows)
        self.assertIn(str(doc.stats.views), rows)
        self.assertIn(str(map_brand_safety_score(doc.brand_safety.overall_score)), rows)
        self.assertIn(bs_category.name, rows)
        self.assertIn(age_group.age_group, rows)
        self.assertIn(gender.gender, rows)
        self.assertIn(content_type.content_type, rows)
        self.assertIn(content_quality.quality, rows)
        self.assertIn(f"https://www.youtube.com/watch?v={doc.main.id}", rows)
        self.video_manager.delete([doc.main.id for doc in docs])

    @mock_s3
    def test_generate_channel_export_data(self):
        """ Test channel export contains all data """
        bs_category = BadWordCategory.objects.first()
        content_type = AuditContentType.objects.first()
        content_quality = AuditContentQuality.objects.first()
        age_group = AuditAgeGroup.objects.first()
        gender = AuditGender.objects.first()

        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        _id = next(int_iterator)
        doc = ChannelManager.model(f"id_{_id}")
        doc.populate_general_data(
            title=f"title_{_id}", top_lang_code="es", iab_categories=["Test_Again"],
            country_code="CA",
        )
        doc.populate_stats(subscribers=888)
        doc.populate_brand_safety(overall_score=44)
        doc.populate_task_us_data(
            brand_safety=[bs_category.id],
            age_group=age_group.id,
            gender=gender.id,
            content_type=content_type.id,
            content_quality=content_quality.id,
            last_vetted_at=timezone.now(),
            mismatched_language=True,
        )
        docs = [doc]
        self.channel_manager.upsert(docs)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, uuid=uuid4(), list_type=0
        )
        generate_segment(segment, Q(), len(docs))
        export_key = segment.get_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        rows = ",".join([row.decode("utf-8") for row in body])

        self.assertIn(doc.general_data.title, rows)
        self.assertIn(doc.general_data.country_code, rows)
        self.assertIn(str(doc.stats.subscribers), rows)
        self.assertIn(str(map_brand_safety_score(doc.brand_safety.overall_score)), rows)
        self.assertIn(bs_category.name, rows)
        self.assertIn(age_group.age_group, rows)
        self.assertIn(gender.gender, rows)
        self.assertIn(content_type.content_type, rows)
        self.assertIn(content_quality.quality, rows)
        self.assertIn(f"https://www.youtube.com/channel/{doc.main.id}", rows)
        self.video_manager.delete([doc.main.id for doc in docs])

    @mock_s3
    def test_generate_video_source_inclusion(self):
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        # Prepare docs to build segment
        docs = []
        for _ in range(5):
            _id = next(int_iterator)
            doc = VideoManager.model(f"id_{_id}")
            doc.populate_general_data(title=f"title_{_id}", age_restricted=False)
            docs.append(doc)
        # Prepare inclusion source list of urls
        source_file = io.BytesIO()
        source_key = f"source_{next(int_iterator)}"
        inclusion = docs[:3]
        exclusion = docs[3:]
        inclusion_urls = [f"https://www.youtube.com/watch?v={doc.main.id}".encode("utf-8") for doc in inclusion]
        source_file.write(b"URL\n")
        source_file.write(b"\n".join(inclusion_urls))
        source_file.seek(0)
        conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, source_key).put(Body=source_file)
        self.video_manager.upsert(docs)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=0, uuid=uuid4(), list_type=0
        )
        CustomSegmentSourceFileUpload.objects.create(
            segment=segment, source_type=SourceListType.INCLUSION.value, filename=source_key,
        )
        generate_segment(segment, Q(), len(docs))
        export_key = segment.get_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        rows = ",".join([row.decode("utf-8") for row in body])
        for included in inclusion:
            self.assertIn(included.main.id, rows)
        for excluded in exclusion:
            self.assertNotIn(excluded.main.id, rows)
        self.video_manager.delete([doc.main.id for doc in docs])

    @mock_s3
    def test_generate_channel_source_inclusion(self):
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        # Prepare docs to build segment
        docs = []
        for i in range(10):
            _id = next(int_iterator)
            doc = ChannelManager.model(f"channel_id_{_id}")
            doc.populate_general_data(title=f"channel_title_{_id}")
            if i % 2 == 0:
                doc.populate_monetization(is_monetizable=True)
            else:
                doc.populate_monetization(is_monetizable=False)
            docs.append(doc)
        # Prepare inclusion source list of urls
        source_file = io.BytesIO()
        source_key = f"source_{next(int_iterator)}"
        half = len(docs) // 2
        inclusion = docs[:half]
        exclusion = docs[half:]
        inclusion_urls = [f"https://www.youtube.com/channel/{doc.main.id}".encode("utf-8") for doc in inclusion]
        source_file.write(b"URL\n")
        source_file.write(b"\n".join(inclusion_urls))
        source_file.seek(0)
        conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, source_key).put(Body=source_file)
        self.channel_manager.upsert(docs)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, uuid=uuid4(), list_type=0
        )
        CustomSegmentSourceFileUpload.objects.create(
            segment=segment, source_type=SourceListType.INCLUSION.value, filename=source_key,
        )
        generate_segment(segment, Q(), len(docs))
        export_key = segment.get_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        rows = ",".join([row.decode("utf-8") for row in body])
        for included in inclusion:
            self.assertIn(included.main.id, rows)
        for excluded in exclusion:
            self.assertNotIn(excluded.main.id, rows)
        self.channel_manager.delete([doc.main.id for doc in docs])

    def test_export_s3_key_retrieval(self):
        """
        test that segment.get_s3_key retrieves existing s3 key if available
        """
        user = self.create_admin_user()
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, owner=user,
        )
        old_s3_key = segment.get_s3_key()
        new_s3_key = 'new_s3_key.csv'
        CustomSegmentFileUpload.objects.create(
            segment=segment,
            query={"params": {"some": "params"}},
            download_url="some-download-url.com/asdf/asdf/asdf.csv",
            filename=new_s3_key
        )
        self.assertEqual(new_s3_key, segment.get_s3_key())
        self.assertNotEqual(new_s3_key, old_s3_key)

    def test_source_s3_key_retrieval(self):
        """
        test that segment.get_source_s3_key retrieves existing s3 key if available
        """
        user = self.create_admin_user()
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, owner=user,
        )
        old_s3_key = segment.get_source_s3_key()
        new_s3_key = 'new_s3_key.csv'
        CustomSegmentSourceFileUpload.objects.create(
            segment=segment,
            source_type=SourceListType.INCLUSION.value,
            filename=new_s3_key
        )
        self.assertEqual(new_s3_key, segment.get_source_s3_key())
        self.assertNotEqual(new_s3_key, old_s3_key)

    @mock_s3
    def test_writes_header_once(self):
        """ Test that header is only written once when batch is empty due to source urls not being in batch """
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        # Prepare docs to build segment
        docs = []
        for i in range(5):
            _id = next(int_iterator)
            doc = ChannelManager.model(f"channel_id_{_id}")
            doc.populate_general_data(title=f"channel_title_{_id}")
            if i % 2 == 0:
                doc.populate_monetization(is_monetizable=True)
            else:
                doc.populate_monetization(is_monetizable=False)
            docs.append(doc)
        # Prepare inclusion source list of urls
        source_file = io.BytesIO()
        source_key = f"source_{next(int_iterator)}"
        half = len(docs) // 2
        inclusion = docs[:half]
        exclusion = docs[half:]
        inclusion_urls = [f"https://www.youtube.com/channel/{doc.main.id}".encode("utf-8") for doc in inclusion]
        source_file.write(b"URL\n")
        source_file.write(b"\n".join(inclusion_urls))
        source_file.seek(0)
        conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, source_key).put(Body=source_file)
        self.channel_manager.upsert(docs)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, uuid=uuid4(), list_type=0
        )
        CustomSegmentSourceFileUpload.objects.create(
            segment=segment, source_type=SourceListType.INCLUSION.value, filename=source_key,
        )
        with patch("segment.tasks.generate_segment.bulk_search", return_value=[[], inclusion, exclusion]):
            generate_segment(segment, Q(), len(docs))
        export_key = segment.get_s3_key()
        body = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"]
        columns = ",".join(segment.export_serializer.columns)
        rows = ",".join([row.decode("utf-8") for row in body])
        for included in inclusion:
            self.assertIn(included.main.id, rows)
        for excluded in exclusion:
            self.assertNotIn(excluded.main.id, rows)
        self.channel_manager.delete([doc.main.id for doc in docs])
        self.assertEqual(rows.count(columns), 1)
