from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_404_NOT_FOUND

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.custom_segment_export_generator import CustomSegmentExportGenerator
from userprofile.models import UserProfile
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentDeleteApiViewV2TestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_LIST,
                       kwargs=dict(segment_type=segment_type))

    def test_not_found(self):
        self.create_test_user()
        CustomSegment.objects.create(id=1, list_type=0, segment_type=0, title="test_1")
        response = self.client.delete(
            self._get_url("video") + "2/"
        )
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_not_found_not_owned(self):
        user = self.create_test_user()
        CustomSegment.objects.create(owner=user, id=1, list_type=0, segment_type=0, title="test_1")
        CustomSegment.objects.create(id=2, list_type=0, segment_type=0, title="test_1")
        response = self.client.delete(
            self._get_url("video") + "2/"
        )
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success(self):
        user = self.create_test_user()
        segment = CustomSegment.objects.create(id=2, owner=user, list_type=0, segment_type=0, title="test_1")
        CustomSegmentFileUpload.objects.create(segment=segment, query={})
        with patch.object(CustomSegmentExportGenerator, "delete_export", lambda foo, bar, baz: None):
            response = self.client.delete(
                self._get_url("video") + "2/"
            )
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)