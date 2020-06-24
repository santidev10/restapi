import uuid
from time import sleep
from unittest.mock import patch

from django.urls import reverse
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND

from es_components.constants import SEGMENTS_UUID_FIELD
from es_components.query_builder import QueryBuilder
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from segment.api.tests.test_brand_safety_preview import PersistentSegmentPreviewApiViewTestCase
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from utils.unittests.test_case import ExtendedAPITestCase


class SegmentDeleteApiViewV2TestCase(ExtendedAPITestCase, ESTestCase):
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

    @patch("segment.models.CustomSegment.delete_export")
    def test_success(self, mock_delete_export):
        mock_delete_export.return_value = {}
        user = self.create_test_user()
        segment_uuid = uuid.uuid4()
        segment = CustomSegment.objects.create(uuid=segment_uuid, owner=user, list_type=0, segment_type=0,
                                               title="test_1")
        CustomSegmentFileUpload.objects.create(segment=segment, query={})

        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(5, "channel", str(segment.uuid))
        segment.es_manager.upsert(mock_data)
        sleep(1)
        response = self.client.delete(
            self._get_url("video") + f"{segment.id}/"
        )
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)
        query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(segment_uuid).get()
        items = segment.es_manager.search(query=query).execute()
        self.assertEqual(items.hits.total.value, 0)

    def test_reject_segment_audit(self):
        """ Segments with audit vetting enabled can not be deleted """
        user = self.create_test_user()
        CustomSegment.objects.create(owner=user, uuid=uuid.uuid4(), id=1, list_type=0, segment_type=0, title="test_1",
                                     audit_id=1)
        CustomSegment.objects.create(id=2, uuid=uuid.uuid4(), list_type=0, segment_type=0, title="test_1")
        response = self.client.delete(
            self._get_url("video") + "1/"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
