from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from saas.urls.namespaces import Namespace
from segment.api.serializers.custom_segment_update_serializer import CustomSegmentUpdateSerializer
from segment.api.urls.names import Name
from segment.models.constants import CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
from segment.models.constants import CUSTOM_SEGMENT_FEATURED_IMAGE_URL_KEY
from segment.models.custom_segment import CustomSegment
from utils.unittests.s3_mock import mock_s3
from utils.unittests.test_case import ExtendedAPITestCase
import json
import uuid

class CustomSegmentUpdateApiViewV1TestCase(ExtendedAPITestCase):

    def _get_url(self, reverse_args=None):
        return reverse(f"{Namespace.SEGMENT}:{Name.CUSTOM_SEGMENT_UPDATE}", args=reverse_args)

    def _create_custom_segment(self, owner):
        uuid_id = uuid.uuid4()
        return CustomSegment.objects.create(**{
            'uuid': uuid_id,
            'title': f"testing custom segment update api view v1: {uuid_id}",
            'owner': owner,
            'segment_type': 0,
        })

    def test_partial_update(self):
        user = self.create_test_user()
        segment = self._create_custom_segment(owner=user)
        payload = {
            "is_featured": True,
            "is_regenerating": True,
        }
        response = self.client.patch(
            self._get_url(reverse_args=[segment.id]), json.dumps(payload), content_type="application/json"
        )
        segment.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        for key, value in payload.items():
            if key.lower() == CustomSegmentUpdateSerializer.FEATURED_IMAGE_FIELD_NAME:
                continue # this is a write-only field
            self.assertEqual(getattr(segment, key, None), value)
        self.assertEqual(
            response.data[CustomSegmentUpdateSerializer.FEATURED_IMAGE_URL_FIELD_NAME],
            CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
        )

    @mock_s3
    def test_image_upload(self):
        user = self.create_test_user()
        segment = self._create_custom_segment(owner=user)
        small_gif = (
            b'\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x00\x00\x00\x21\xf9\x04'
            b'\x01\x0a\x00\x01\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02'
            b'\x02\x4c\x01\x00\x3b'
        )
        image = SimpleUploadedFile("small_gif.gif", small_gif, content_type="image/gif")
        payload = {CustomSegmentUpdateSerializer.FEATURED_IMAGE_FIELD_NAME: image,}
        response = self.client.patch(self._get_url(reverse_args=[segment.id]), payload)
        segment.refresh_from_db()
        res_featured_image_url = response.data[CustomSegmentUpdateSerializer.FEATURED_IMAGE_URL_FIELD_NAME]
        self.assertNotEqual(res_featured_image_url, CUSTOM_SEGMENT_DEFAULT_IMAGE_URL)
        self.assertIn(str(segment.uuid), res_featured_image_url)
        self.assertEqual(segment.featured_image_url, res_featured_image_url)
