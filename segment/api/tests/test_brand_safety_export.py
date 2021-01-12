from io import BytesIO

import boto3
from django.conf import settings
from django.urls import reverse
from moto import mock_s3
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
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
    def test_non_master_channel_success(self):
        self.create_admin_user()
        segment = CustomSegment.objects.create(segment_type=1, title="test_channel")
        CustomSegmentFileUpload.objects.create(
            segment=segment,
            admin_filename=segment.get_admin_s3_key(),
            query={}
        )
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        s3_key = segment.get_admin_s3_key()
        s3_obj = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, s3_key)
        file = BytesIO(",".join(segment.admin_export_serializer.columns).encode("utf-8"))
        file.seek(0)
        s3_obj.put(Body=file)
        response = self.client.get(self._get_url("channel", segment.id) + "?is_master=false")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(",".join(row.decode("utf-8") for row in response), ",".join(segment.admin_export_serializer.columns))

    @mock_s3
    def test_non_master_video_success(self):
        self.create_admin_user()
        segment = CustomSegment.objects.create(segment_type=0, title="test_video")
        CustomSegmentFileUpload.objects.create(
            segment=segment,
            admin_filename=segment.get_admin_s3_key(),
            query={}
        )
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        s3_key = segment.get_admin_s3_key()
        s3_obj = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, s3_key)
        file = BytesIO(",".join(segment.admin_export_serializer.columns).encode("utf-8"))
        file.seek(0)
        s3_obj.put(Body=file)
        response = self.client.get(self._get_url("video", segment.id) + "?is_master=false")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(",".join(row.decode("utf-8") for row in response), ",".join(segment.admin_export_serializer.columns))