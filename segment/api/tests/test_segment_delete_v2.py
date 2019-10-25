from unittest.mock import patch

from django.urls import reverse
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_404_NOT_FOUND
import uuid

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentDeleteApiViewV2TestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_LIST,
                       kwargs=dict(segment_type=segment_type))

    def test_not_found(self):
        self.create_test_user()
        CustomSegment.objects.create(id=1, uuid=uuid.uuid4(), list_type=0, segment_type=0, title="test_1")
        response = self.client.delete(
            self._get_url("video") + "2/"
        )
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_not_found_not_owned(self):
        user = self.create_test_user()
        CustomSegment.objects.create(owner=user, uuid=uuid.uuid4(), id=1, list_type=0, segment_type=0, title="test_1")
        CustomSegment.objects.create(id=2, uuid=uuid.uuid4(), list_type=0, segment_type=0, title="test_1")
        response = self.client.delete(
            self._get_url("video") + "2/"
        )
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    @patch("segment.models.CustomSegment.remove_all_from_segment")
    def test_success(self, mock_delete_export, mock_remove_from_segment):
        mock_delete_export.return_value = {}
        mock_remove_from_segment.return_value = {}

        user = self.create_test_user()
        segment = CustomSegment.objects.create(id=2, uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="test_1")
        CustomSegmentFileUpload.objects.create(segment=segment, query={})

        response = self.client.delete(
            self._get_url("video") + "2/"
        )
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)
