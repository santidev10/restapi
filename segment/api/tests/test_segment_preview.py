from unittest.mock import MagicMock
from unittest.mock import patch

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
import uuid

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.custom_segment import CustomSegment
from segment.api.views.brand_safety.brand_safety_list import PersistentSegmentListApiView
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentPreviewApiViewTestCase(ExtendedAPITestCase):
    THRESHOLD = PersistentSegmentListApiView.MINIMUM_ITEMS_COUNT

    def _get_url(self, segment_type, segment_uuid):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_PREVIEW,
                       kwargs=dict(segment_type=segment_type, uuid=segment_uuid))

    def test_persistent_segment_channel_segment_preview(self):
        self.create_admin_user()
        segment = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",

        )
        with patch("segment.api.views.shared.segment_preview.ChannelWithBlackListSerializer"), \
                patch("segment.api.views.shared.segment_preview.ChannelManager"):
            url = self._get_url("channel", segment.uuid)
            response = self.client.get(url)
            print(response)
            self.assertEqual(len(response.data["items"]), 5)
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_persistent_segment_video_segment_preview(self):
        self.create_admin_user()
        segment = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",

        )
        with patch("segment.api.views.shared.segment_preview.VideoWithBlackListSerializer"), \
                patch("segment.api.views.shared.segment_preview.VideoManager"):
            url = self._get_url("video", segment.uuid) + "?size=10"
            response = self.client.get(url)
            self.assertEqual(len(response.data["items"]), 10)
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_custom_channel_segment_preview(self):
        self.create_admin_user()
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            segment_type="channel"

        )
        with patch("segment.api.views.shared.segment_preview.ChannelWithBlackListSerializer"), \
                patch("segment.api.views.shared.segment_preview.ChannelManager"):
            url = self._get_url("channel", segment.uuid)
            response = self.client.get(url)
            print(response)
            self.assertEqual(len(response.data["items"]), 5)
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_custom_video_segment_preview(self):
        self.create_admin_user()
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            segment_type="video"

        )
        with patch("segment.api.views.shared.segment_preview.VideoWithBlackListSerializer"), \
                patch("segment.api.views.shared.segment_preview.VideoManager"):
            url = self._get_url("video", segment.uuid) + "?size=10"
            response = self.client.get(url)
            self.assertEqual(len(response.data["items"]), 10)
            self.assertEqual(response.status_code, HTTP_200_OK)