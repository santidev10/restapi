import uuid
from time import sleep
from unittest.mock import patch

from django.urls import reverse
from moto import mock_s3
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from es_components.constants import SEGMENTS_UUID_FIELD
from es_components.query_builder import QueryBuilder
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from segment.api.tests.test_brand_safety_preview import PersistentSegmentPreviewApiViewTestCase
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models import SegmentAction
from segment.models.constants import SegmentActionEnum
from segment.models.constants import SegmentTypeEnum
from segment.models.utils.segment_exporter import SegmentExporter
from utils.unittests.test_case import ExtendedAPITestCase
from utils.datetime import now_in_default_tz
from utils.unittests.patch_bulk_create import patch_bulk_create
from userprofile.constants import StaticPermissions


@patch("segment.models.models.safe_bulk_create", new=patch_bulk_create)
class SegmentDeleteApiViewTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, segment_type, pk):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_DELETE,
                       kwargs=dict(segment_type=segment_type, pk=str(pk)))

    def test_not_found(self):
        self.create_admin_user()
        CustomSegment.objects.create(id=1, uuid=uuid.uuid4(), list_type=0, segment_type=0, title="test_1")
        response = self.client.delete(
            self._get_url("video", "2")
        )
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_forbidden(self):
        user = self.create_test_user()
        CustomSegment.objects.create(owner=user, uuid=uuid.uuid4(), id=1, segment_type=0, title="test_1")
        CustomSegment.objects.create(id=2, uuid=uuid.uuid4(), segment_type=0, title="test_1")
        response = self.client.delete(
            self._get_url("video", "2")
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_delete_permission_forbidden(self):
        """
        don't allow deletes if the the user has a delete permission, but for the wrong type
        :return:
        """
        self.create_test_user(perms={StaticPermissions.BUILD__CTL_DELETE_CHANNEL_LIST: True})
        CustomSegment.objects.create(uuid=uuid.uuid4(), id=1, segment_type=SegmentTypeEnum.VIDEO.value, title="test_1")
        response = self.client.delete(
            self._get_url("video", "1")
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

        self.create_test_user(perms={StaticPermissions.BUILD__CTL_DELETE_VIDEO_LIST: True})
        CustomSegment.objects.create(uuid=uuid.uuid4(), id=2, segment_type=SegmentTypeEnum.CHANNEL.value,
                                     title="test_2")
        response = self.client.delete(
            self._get_url("video", "2")
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    @patch("segment.models.utils.segment_exporter.SegmentExporter.delete_export")
    def test_delete_permission_success(self, mock_delete_export):
        """
        allow deletes only if the user has the delete permission for that segment type or if they are the owner
        :return:
        """
        user = self.create_test_user()
        CustomSegment.objects.create(owner=user, uuid=uuid.uuid4(), id=1, segment_type=SegmentTypeEnum.VIDEO.value,
                                     title="test_owner")
        response = self.client.delete(
            self._get_url("video", "1")
        )
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)

        self.create_test_user(perms={StaticPermissions.BUILD__CTL_DELETE_VIDEO_LIST: True})
        CustomSegment.objects.create(uuid=uuid.uuid4(), id=2, segment_type=SegmentTypeEnum.VIDEO.value,
                                     title="test_video")
        response = self.client.delete(
            self._get_url("video", "2")
        )
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)

        self.create_test_user(perms={StaticPermissions.BUILD__CTL_DELETE_CHANNEL_LIST: True})
        CustomSegment.objects.create(uuid=uuid.uuid4(), id=3, segment_type=SegmentTypeEnum.CHANNEL.value,
                                     title="test_channel")
        response = self.client.delete(
            self._get_url("video", "3")
        )
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)

    @patch("segment.models.utils.segment_exporter.SegmentExporter.delete_export")
    def test_success(self, mock_delete_export):
        mock_delete_export.return_value = {}
        user = self.create_test_user(perms={StaticPermissions.BUILD__CTL_DELETE_VIDEO_LIST: True})
        segment_uuid = uuid.uuid4()
        segment = CustomSegment.objects.create(uuid=segment_uuid, owner=user, list_type=0,
                                               segment_type=SegmentTypeEnum.VIDEO.value, title="test_1")
        CustomSegmentFileUpload.objects.create(segment=segment, query={})

        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(5, "channel", str(segment.uuid))
        segment.es_manager.upsert(mock_data)
        sleep(1)
        response = self.client.delete(
            self._get_url("video", segment.id)
        )
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)
        query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(segment_uuid).get()
        items = segment.es_manager.search(query=query).execute()
        self.assertEqual(items.hits.total.value, 0)

    def test_reject_segment_audit(self):
        """ Segments with audit vetting enabled can not be deleted """
        user = self.create_test_user(perms={StaticPermissions.BUILD__CTL_DELETE_VIDEO_LIST: True})
        CustomSegment.objects.create(owner=user, uuid=uuid.uuid4(), id=1, list_type=0,
                                     segment_type=SegmentTypeEnum.VIDEO.value, title="test_1",
                                     audit_id=1)
        CustomSegment.objects.create(id=2, uuid=uuid.uuid4(), list_type=0, segment_type=SegmentTypeEnum.VIDEO.value,
                                     title="test_1")
        response = self.client.delete(
            self._get_url("video", 1)
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    @mock_s3
    def test_creates_delete_action(self):
        """ Test creating CTL creates DELETE action """
        now = now_in_default_tz()
        user = self.create_admin_user()
        ctl = CustomSegment.objects.create(owner=user, segment_type=0, title="test_1")
        with patch.object(SegmentExporter, "delete_export"):
            response = self.client.delete(self._get_url("video", ctl.id))
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)
        action = SegmentAction.objects.get(user=user, action=SegmentActionEnum.DELETE.value)
        self.assertTrue(action.created_at > now)
