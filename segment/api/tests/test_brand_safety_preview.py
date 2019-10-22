from unittest.mock import MagicMock
from unittest.mock import patch

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
import uuid

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.int_iterator import int_iterator


class SegmentPreviewApiViewTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type, pk):
        return reverse(Namespace.SEGMENT + ":" + Name.PERSISTENT_SEGMENT_PREVIEW,
                       kwargs=dict(segment_type=segment_type, pk=pk))

    @staticmethod
    def get_mock_data(count, data_type="video"):
        data = []
        for i in range(count):
            item = {
                "main": {
                    "id": data_type + str(i),
                    "brand_safety": {
                        "overall_score": 100 - i
                    }
                }
            }
            if data_type == "channel":
                item["brand_safety"]["video_scored"] = i
            data.append(item)
        return data

    def test_persistent_segment_channel_segment_preview(self):
        self.create_admin_user()
        items = 5
        _id = next(int_iterator)
        segment = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=_id,
        )
        with patch("segment.api.views.brand_safety.brand_safety_preview.SegmentListAPIViewAdapter.get_queryset"), \
                patch("segment.api.views.brand_safety.brand_safety_preview.SegmentListAPIViewAdapter.get_queryset") as serializer, \
                patch("segment.api.views.shared.segment_preview.ChannelManager"):
            url = self._get_url("channel", segment.id)
            serializer.data = self.get_mock_data(items, "channel")
            print(serializer.data)
            response = self.client.get(url)
            print(response.data)
            self.assertEqual(len(response.data["items"]), items)
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_persistent_segment_video_segment_preview(self):
        self.create_admin_user()
        segment = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",

        )
        with patch("segment.api.views.brand_safety.brand_safety_preview.SegmentListAPIViewAdapter.get_queryset") as get_queryset, \
                patch("segment.api.views.shared.segment_preview.VideoManager"):
            url = self._get_url("video", segment.id) + "?size=10"
            response = self.client.get(url)
            self.assertEqual(len(response.data["items"]), 10)
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_persistent_segment_channel_segment_preview_sorted(self):
        self.create_admin_user()
        segment = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",

        )
        with patch("segment.api.views.brand_safety.brand_safety_preview.SegmentListAPIViewAdapter.get_queryset") as get_queryset, \
                patch("segment.api.views.shared.segment_preview.ChannelManager"):
            url = self._get_url("channel", segment.id)
            response = self.client.get(url)
            self.assertEqual(len(response.data["items"]), 5)
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_persistent_segment_channel_segment_preview(self):
        self.create_admin_user()
        segment = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",

        )
        with patch("segment.api.views.brand_safety.brand_safety_preview.SegmentListAPIViewAdapter.get_queryset") as get_queryset, \
                patch("segment.api.views.shared.segment_preview.VideoManager"):
            url = self._get_url("video", segment.id) + "?size=10"
            response = self.client.get(url)
            self.assertEqual(len(response.data["items"]), 10)
            self.assertEqual(response.status_code, HTTP_200_OK)
