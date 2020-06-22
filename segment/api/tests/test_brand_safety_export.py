from io import BytesIO

import boto3
from django.conf import settings
from django.urls import reverse
from moto import mock_s3
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN
from uuid import uuid4

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import PersistentSegmentChannel
from segment.models import PersistentSegmentVideo
from segment.models.persistent.base import PersistentSegmentFileUpload
from utils.unittests.test_case import ExtendedAPITestCase


class BrandSafetyListExportAPIViewTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type, pk):
        return reverse(Namespace.SEGMENT + ":" + Name.PERSISTENT_SEGMENT_EXPORT,
                       kwargs=dict(segment_type=segment_type, pk=pk))

    def test_permission_fail(self):
        self.create_test_user()
        response = self.client.get(self._get_url("channel", "1"))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @mock_s3
    def test_is_master_channel_success(self):
        self.create_admin_user()
        segment = PersistentSegmentChannel.objects.create(is_master=True, uuid=uuid4())
        export = PersistentSegmentFileUpload.objects.create(segment_uuid=segment.uuid, filename=f"{uuid4()}.csv")
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=settings.AMAZON_S3_BUCKET_NAME)
        s3_obj = conn.Object(settings.AMAZON_S3_BUCKET_NAME, export.filename)
        file = BytesIO(",".join(segment.get_export_columns()).encode("utf-8"))
        file.seek(0)
        s3_obj.put(Body=file)
        response = self.client.get(self._get_url("channel", segment.id) + "?is_master=true")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(",".join(row.decode("utf-8") for row in response), ",".join(segment.get_export_columns()))

    @mock_s3
    def test_is_master_video_success(self):
        self.create_admin_user()
        segment = PersistentSegmentVideo.objects.create(is_master=True, uuid=uuid4())
        export = PersistentSegmentFileUpload.objects.create(segment_uuid=segment.uuid, filename=f"{uuid4()}.csv")
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=settings.AMAZON_S3_BUCKET_NAME)
        s3_obj = conn.Object(settings.AMAZON_S3_BUCKET_NAME, export.filename)
        file = BytesIO(",".join(segment.get_export_columns()).encode("utf-8"))
        file.seek(0)
        s3_obj.put(Body=file)
        response = self.client.get(self._get_url("video", segment.id) + "?is_master=true")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(",".join(row.decode("utf-8") for row in response), ",".join(segment.get_export_columns()))

    @mock_s3
    def test_non_master_channel_success(self):
        self.create_admin_user()
        segment = CustomSegment.objects.create(segment_type=1, title="test_channel")
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        s3_key = segment.get_s3_key()
        s3_obj = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, s3_key)
        file = BytesIO(",".join(segment.serializer.columns).encode("utf-8"))
        file.seek(0)
        s3_obj.put(Body=file)
        response = self.client.get(self._get_url("channel", segment.id) + "?is_master=false")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(",".join(row.decode("utf-8") for row in response), ",".join(segment.serializer.columns))

    @mock_s3
    def test_non_master_video_success(self):
        self.create_admin_user()
        segment = CustomSegment.objects.create(segment_type=0, title="test_video")
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        s3_key = segment.get_s3_key()
        s3_obj = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, s3_key)
        file = BytesIO(",".join(segment.serializer.columns).encode("utf-8"))
        file.seek(0)
        s3_obj.put(Body=file)
        response = self.client.get(self._get_url("video", segment.id) + "?is_master=false")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(",".join(row.decode("utf-8") for row in response), ",".join(segment.serializer.columns))