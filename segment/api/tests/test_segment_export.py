from mock import patch

from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.response import Response
import uuid

from audit_tool.models import AuditProcessor
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator
from utils.aws.s3_exporter import ReportNotFoundException


class SegmentExportAPIViewTestCase(ExtendedAPITestCase):
    def _get_url(self, pk):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_EXPORT, kwargs=dict(pk=pk))

    def _create_segment(self, segment_params=None, export_params=None):
        default_segment_params = dict(
            list_type=0,
            segment_type=0,
            uuid=uuid.uuid4(),
            title="test"
        )
        default_segment_params.update(segment_params if segment_params else {})
        export_params = export_params if export_params else dict(query={})
        if "uuid" not in segment_params:
            segment_params["uuid"] = uuid.uuid4()
        segment = CustomSegment.objects.create(**default_segment_params)
        export = CustomSegmentFileUpload.objects.create(segment=segment, **export_params)
        return segment, export

    def _create_user(self, user_data=None):
        user_data = user_data if user_data else dict(email=f"test{next(int_iterator)}@test.com")
        user = get_user_model().objects.create(**user_data)
        return user

    def test_reject_vetting(self):
        """ Users without vetting admin permissions should not be able to export vetting lists """
        test_user = self._create_user()
        self.create_test_user()
        segment, export = self._create_segment(segment_params=dict(owner=test_user))
        with patch("segment.api.views.custom_segment.segment_export.StreamingHttpResponse", return_value=Response()),\
                patch.object(CustomSegment, "get_export_file") as mock_export:
            url = self._get_url(segment.id) + "?vetted=true"
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(mock_export.call_count, 0)

    def test_reject_vetting_progress(self):
        """ Users without vetting admin permissions should not be able to export vetting lists progress """
        user = self._create_user()
        self.create_test_user()
        segment, export = self._create_segment(segment_params=dict(owner=user))
        with patch("segment.api.views.custom_segment.segment_export.SegmentExport._vetted_items_generator") as mock_generate:
            url = self._get_url(segment.id) + "?vetted=true"
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(mock_generate.call_count, 0)

    def test_vetting_admin_export_success(self):
        """ Admin users should be able to export all lists """
        test_user = self._create_user()
        self.create_admin_user()
        segment, export = self._create_segment(segment_params=dict(owner=test_user))
        with patch("segment.api.views.custom_segment.segment_export.StreamingHttpResponse", return_value=Response()),\
                patch.object(CustomSegment, "get_export_file") as mock_export:
            response = self.client.get(self._get_url(segment.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        mock_export.assert_called_once()

    def test_vetting_admin_export_progress_success(self):
        """ Admin users should be able to export progress of all lists """
        test_user = self._create_user()
        self.create_admin_user()
        audit = AuditProcessor.objects.create(source=1)
        segment, export = self._create_segment(segment_params=dict(owner=test_user, audit_id=audit.id))
        with patch("segment.api.views.custom_segment.segment_export.StreamingHttpResponse", return_value=Response()), \
                patch("segment.api.views.custom_segment.segment_export.SegmentExport._vetted_items_generator") as mock_generate, \
                patch.object(CustomSegment, "get_export_file") as mock_export:
            url = self._get_url(segment.id) + "?vetted=true"
            mock_export.side_effect = ReportNotFoundException
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        mock_generate.assert_called_once()
        mock_export.assert_called_once()
