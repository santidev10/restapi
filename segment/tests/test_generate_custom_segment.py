from mock import patch
from uuid import uuid4

import boto3
from django.conf import settings
from moto import mock_s3

from es_components.tests.utils import ESTestCase
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.tasks.generate_segment import CTLGenerateException
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class GenerateCustomSegmentTestCase(ExtendedAPITestCase, ESTestCase):
    @mock_s3
    def test_fails_sets_error(self):
        """ Test that if task fails, sets errors message """
        user = self.create_test_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        segment = CustomSegment.objects.create(
            title=f"ti tle_{next(int_iterator)}",
            segment_type=1, owner=user, uuid=uuid4(), list_type=0,
        )
        # segment has no segment.export (CustomSegmentFileUpload)
        generate_custom_segment(segment.id)
        segment.refresh_from_db()
        self.assertTrue(segment.statistics["error"])

    @mock_s3
    def test_source_list_fails_sets_error(self):
        """ Test error is set if retrieving source list fails """
        user = self.create_test_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, owner=user, uuid=uuid4(), list_type=0,
        )
        CustomSegmentFileUpload.objects.create(query={"body": {}}, segment=segment)
        mock_exception = CTLGenerateException("Unable to process source list")
        with patch("segment.tasks.generate_custom_segment.generate_segment", side_effect=mock_exception):
            generate_custom_segment(segment.id)
        segment.refresh_from_db()
        self.assertTrue("source" in segment.statistics["error"])

    @mock_s3
    def test_uncaught_exception(self):
        user = self.create_test_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        segment = CustomSegment.objects.create(
            title=f"title_{next(int_iterator)}",
            segment_type=1, owner=user, uuid=uuid4(), list_type=0,
        )
        with patch("segment.tasks.generate_custom_segment.generate_segment", side_effect=Exception):
            generate_custom_segment(segment.id)
        segment.refresh_from_db()
        self.assertTrue("Unable" in segment.statistics["error"])
