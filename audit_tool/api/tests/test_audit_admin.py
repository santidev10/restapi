import json
from uuid import uuid4

from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from audit_tool.api.urls.names import AuditPathName
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoVet
from userprofile.models import StaticPermissions
from saas.urls.namespaces import Namespace
from segment.models import CustomSegmentVettedFileUpload
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase

CHANNEL_PREFIX = "https://www.youtube.com/channel/"
VIDEO_PREFIX = "https://www.youtube.com/watch?v="
VIDEO_PREFIX2 = "https://www.youtube.com/video/"


class AuditAdminTestCase(ExtendedAPITestCase):
    custom_segment_model = None
    custom_segment_export_model = None

    def _get_url(self):
        url = reverse(AuditPathName.AUDIT_ADMIN, [Namespace.AUDIT_TOOL])
        return url

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Import and set models to avoid recursive ImportError
        from segment.models import CustomSegment
        from segment.models import CustomSegmentFileUpload
        cls.custom_segment_model = CustomSegment
        cls.custom_segment_export_model = CustomSegmentFileUpload

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def _create_segment_audit(self, user, audit_params=None, segment_params=None):
        default_audit_params = dict(source=1, audit_type=2, params=dict(instructions="test instructions"))
        default_segment_params = dict(
            owner=user, title="test", segment_type=0, list_type=0, statistics={"items_count": 1}, uuid=uuid4()
        )
        default_audit_params.update(audit_params or {})
        default_segment_params.update(segment_params or {})
        audit = AuditProcessor.objects.create(**default_audit_params)
        segment = self.custom_segment_model.objects.create(audit_id=audit.id, **default_segment_params)
        self.custom_segment_export_model.objects.create(segment=segment, query={})
        return audit, segment

    def test_success_report_videos(self):
        """
        Admin reporting of vetting items should reset AuditProcessor
            completed and AuditVideoVet objects vetting fields
        """
        user = self.create_test_user(perms={
            StaticPermissions.BUILD__CTL_REPORT_VETTING_ISSUE: True,
        })
        now = timezone.now()
        audit, segment = self._create_segment_audit(
            user,
            audit_params=dict(audit_type=1, completed=now),
            segment_params=dict(segment_type=0)
        )
        audit.refresh_from_db()
        test_video_audits = [AuditVideo(video_id=f"{idx}test_video_id"[:11]) for idx in range(10)]
        AuditVideo.objects.bulk_create(test_video_audits)
        test_video_vets = [AuditVideoVet(audit=audit, video=v_audit, processed=now, clean=True) for v_audit in
                           test_video_audits]
        AuditVideoVet.objects.bulk_create(test_video_vets)
        data = {
            "audit_id": audit.id,
            "items_ids": "\n".join([
                VIDEO_PREFIX + item.video_id if i % 2 == 0 else VIDEO_PREFIX2 + item.video_id
                for i, item in enumerate(test_video_audits)
            ])
        }
        CustomSegmentVettedFileUpload.objects.create(segment=segment)
        response = self.client.patch(self._get_url(), json.dumps(data), content_type="application/json")
        [item.refresh_from_db() for item in test_video_vets]
        audit.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(segment.is_vetting_complete, False)
        self.assertTrue(all(item.processed is None for item in test_video_vets))
        self.assertTrue(all(item.clean is None for item in test_video_vets))
        self.assertFalse(CustomSegmentVettedFileUpload.objects.filter(segment=segment).exists())

    def test_success_report_channels(self):
        """
        Admin reporting of vetting items should reset AuditProcessor
            completed and AuditChannelVet vetting fields
        """
        user = self.create_test_user(perms={
            StaticPermissions.BUILD__CTL_REPORT_VETTING_ISSUE: True,
        })
        now = timezone.now()
        audit, segment = self._create_segment_audit(
            user,
            audit_params=dict(audit_type=2, completed=now),
            segment_params=dict(segment_type=1)
        )
        audit.refresh_from_db()
        test_channel_audits = [AuditChannel(channel_id=f"{idx}test_youtube_channel_id"[:24]) for idx in range(10)]
        AuditChannel.objects.bulk_create(test_channel_audits)
        test_channel_vets = [AuditChannelVet(audit=audit, channel=c_audit, processed=now, clean=True) for c_audit in
                             test_channel_audits]
        AuditChannelVet.objects.bulk_create(test_channel_vets)
        data = {
            "audit_id": audit.id,
            "items_ids": "\n".join([CHANNEL_PREFIX + item.channel_id for item in test_channel_audits]),
        }
        CustomSegmentVettedFileUpload.objects.create(segment=segment)
        response = self.client.patch(self._get_url(), json.dumps(data), content_type="application/json")
        [item.refresh_from_db() for item in test_channel_vets]
        audit.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(segment.is_vetting_complete, False)
        self.assertTrue(all(item.processed is None for item in test_channel_vets))
        self.assertTrue(all(item.clean is None for item in test_channel_vets))
        self.assertFalse(CustomSegmentVettedFileUpload.objects.filter(segment=segment).exists())

    def test_reject_permissions(self):
        """
        user must be either an Admin, or have the BUILD__CTL_REPORT_VETTING_ISSUE permission
        :return:
        """
        self.create_test_user(perms={
            StaticPermissions.BUILD__CTL_REPORT_VETTING_ISSUE: False,
        })
        response = self.client.patch(self._get_url())
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_reject_invalid_items_ids(self):
        """ items_ids to be reported should be a newline separated string """
        user = self.create_admin_user()
        audit, segment = self._create_segment_audit(
            user,
            audit_params=dict(audit_type=2),
            segment_params=dict(segment_type=1)
        )
        data = {
            "audit_id": audit.id,
            "items_ids": [1, 2, 3]
        }
        response = self.client.patch(self._get_url(), json.dumps(data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_reject_report_videos_format(self):
        """
        Admin reporting of vetting items should reset AuditProcessor
            completed and AuditVideoVet objects vetting fields
        """
        user = self.create_admin_user()
        now = timezone.now()
        audit, segment = self._create_segment_audit(
            user,
            audit_params=dict(audit_type=1, completed=now),
            segment_params=dict(segment_type=0)
        )
        audit.refresh_from_db()
        test_video_audits = [AuditVideo(video_id=f"{idx}wrong"[:11]) for idx in range(10)]
        AuditVideo.objects.bulk_create(test_video_audits)
        test_video_vets = [AuditVideoVet(audit=audit, video=v_audit, processed=now, clean=True) for v_audit in
                           test_video_audits]
        AuditVideoVet.objects.bulk_create(test_video_vets)
        data = {
            "audit_id": audit.id,
            "items_ids": "\n".join([VIDEO_PREFIX + item.video_id for item in test_video_audits])
        }
        CustomSegmentVettedFileUpload.objects.create(segment=segment)
        response = self.client.patch(self._get_url(), json.dumps(data), content_type="application/json")
        [item.refresh_from_db() for item in test_video_vets]
        audit.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_reject_report_channels_format(self):
        """
        Admin reporting of vetting items should reset AuditProcessor
            completed and AuditChannelVet vetting fields
        """
        user = self.create_admin_user()
        now = timezone.now()
        audit, segment = self._create_segment_audit(
            user,
            audit_params=dict(audit_type=2, completed=now),
            segment_params=dict(segment_type=1)
        )
        audit.refresh_from_db()
        test_channel_audits = [AuditChannel(channel_id=f"test_youtube_channel_id_wrong_length{idx}") for idx in
                               range(10)]
        AuditChannel.objects.bulk_create(test_channel_audits)
        test_channel_vets = [AuditChannelVet(audit=audit, channel=c_audit, processed=now, clean=True) for c_audit in
                             test_channel_audits]
        AuditChannelVet.objects.bulk_create(test_channel_vets)
        data = {
            "audit_id": audit.id,
            "items_ids": "\n".join([CHANNEL_PREFIX + item.channel_id for item in test_channel_audits]),
        }
        CustomSegmentVettedFileUpload.objects.create(segment=segment)
        response = self.client.patch(self._get_url(), json.dumps(data), content_type="application/json")
        [item.refresh_from_db() for item in test_channel_vets]
        audit.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_reject_channel_items_ids(self):
        """ items_ids to be reported should match the type (video, channel) of the CustomSegment """
        user = self.create_admin_user()
        audit, segment = self._create_segment_audit(
            user,
            audit_params=dict(audit_type=2),
            segment_params=dict(segment_type=1)
        )
        # CustomSegment is type 1 (channel)
        # Youtube channel ids are length 24
        test_ids = ",".join([f"test{idx}" for idx in range(10)])
        data = {
            "audit_id": audit.id,
            "items_ids": test_ids
        }
        response = self.client.patch(self._get_url(), json.dumps(data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_reject_video_items_ids(self):
        """ items_ids to be reported should match the type (video, channel) of the CustomSegment """
        user = self.create_admin_user()
        audit, segment = self._create_segment_audit(
            user,
            audit_params=dict(audit_type=1),
            segment_params=dict(segment_type=0)
        )
        # CustomSegment is type 0 (video)
        # Youtube video ids are length 11
        test_ids = "\n".join([f"test_video_id_wrong_length{idx}" for idx in range(10)])
        data = {
            "audit_id": audit.id,
            "items_ids": test_ids
        }
        response = self.client.patch(self._get_url(), json.dumps(data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_segment_not_found(self):
        """ Test handling segment not found """
        user = self.create_admin_user()
        audit, segment = self._create_segment_audit(
            user,
            audit_params=dict(audit_type=2),
            segment_params=dict(segment_type=1)
        )
        segment.audit_id = None
        segment.save()
        data = {
            "audit_id": audit.id,
            "items_ids": ""
        }
        response = self.client.patch(self._get_url(), json.dumps(data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_audit_not_found(self):
        """ Test handling audit not found """
        user = self.create_admin_user()
        self._create_segment_audit(
            user,
            audit_params=dict(audit_type=2),
            segment_params=dict(segment_type=1)
        )
        data = {
            "audit_id": 9999,
            "items_ids": ""
        }
        response = self.client.patch(self._get_url(), json.dumps(data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
