import io
import json
import uuid

import boto3
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse
from moto import mock_s3 as moto_s3
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from saas.urls.namespaces import Namespace
from segment.api.serializers.custom_segment_update_serializers import CustomSegmentAdminUpdateSerializer
from segment.api.urls.names import Name
from segment.models.constants import CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
from segment.models.custom_segment import CustomSegment
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.s3_mock import mock_s3 as mock_s3


class CustomSegmentUpdateApiViewV1TestCase(ExtendedAPITestCase):

    def _get_url(self, reverse_args=None):
        return reverse(f"{Namespace.SEGMENT}:{Name.CUSTOM_SEGMENT_UPDATE}", args=reverse_args)

    def _create_custom_segment(self, owner):
        uuid_id = uuid.uuid4()
        return CustomSegment.objects.create(**{
            "uuid": uuid_id,
            "title": f"testing custom segment update api view v1: {uuid_id}",
            "owner": owner,
            "segment_type": 0,
        })

    @mock_s3
    def test_partial_admin_update(self):
        user = self.create_admin_user()
        segment = self._create_custom_segment(owner=user)
        payload = {
            "title": "new title",
            "is_featured": True,
            "is_regenerating": True,
        }
        response = self.client.patch(
            self._get_url(reverse_args=[segment.id]), json.dumps(payload), content_type="application/json"
        )
        segment.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        for key, value in payload.items():
            self.assertEqual(getattr(segment, key, None), value)
        self.assertEqual(
            response.data[CustomSegmentAdminUpdateSerializer.FEATURED_IMAGE_URL_FIELD_NAME],
            CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
        )

        list_url = reverse(f"{Namespace.SEGMENT_V2}:{Name.SEGMENT_LIST}", args=['video'])
        list_response = self.client.get(list_url)

        self.assertEqual(list_response.status_code, HTTP_200_OK)
        items = list_response.data['items']
        for key in list(payload.keys()):
            with self.subTest(key):
                self.assertEqual(items[0][key], payload[key])

    @mock_s3
    def test_featured_image_upload(self):
        user = self.create_admin_user()
        segment = self._create_custom_segment(owner=user)
        small_gif = (
            b"\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x00\x00\x00\x21\xf9\x04"
            b"\x01\x0a\x00\x01\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02"
            b"\x02\x4c\x01\x00\x3b"
        )
        image = SimpleUploadedFile("small_gif.gif", small_gif, content_type="image/gif")
        payload = {CustomSegmentAdminUpdateSerializer.FEATURED_IMAGE_FIELD_NAME: image, }
        response = self.client.patch(self._get_url(reverse_args=[segment.id]), payload)
        segment.refresh_from_db()
        res_featured_image_url = response.data[CustomSegmentAdminUpdateSerializer.FEATURED_IMAGE_URL_FIELD_NAME]
        self.assertNotEqual(res_featured_image_url, CUSTOM_SEGMENT_DEFAULT_IMAGE_URL)
        self.assertIn(str(segment.uuid), res_featured_image_url)
        self.assertEqual(segment.featured_image_url, res_featured_image_url)

    @mock_s3
    def test_non_admin_owner_update(self):
        user = self.create_test_user()
        segment = self._create_custom_segment(owner=user)
        payload = {
            "title": "new title",
            "is_featured": True,
            "is_regenerating": True,
        }
        response = self.client.patch(
            self._get_url(reverse_args=[segment.id]), json.dumps(payload), content_type="application/json"
        )
        segment.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    @mock_s3
    def test_non_admin_non_owner_update(self):
        owner = get_user_model().objects.create(email="owner@xyz.com")
        self.create_test_user()
        segment = self._create_custom_segment(owner=owner)
        payload = {
            "title": "new title",
            "is_featured": True,
            "is_regenerating": True,
        }
        response = self.client.patch(
            self._get_url(reverse_args=[segment.id]), json.dumps(payload), content_type="application/json"
        )
        segment.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @moto_s3
    def test_update_content_disposition(self):
        """ Test updating title should update object content disposition """
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        user = self.create_admin_user()
        segment = self._create_custom_segment(owner=user)
        export_key = segment.get_s3_key()
        export_file = io.BytesIO()
        conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).put(Body=export_file)
        payload = {
            "title": "new title"
        }
        response = self.client.patch(
            self._get_url(reverse_args=[segment.id]), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        segment.refresh_from_db()

        obj = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()
        self.assertIn(payload["title"], obj["ContentDisposition"])
