from time import sleep

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
import uuid

from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.api.tests.test_brand_safety_preview import PersistentSegmentPreviewApiViewTestCase
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.int_iterator import int_iterator


class SegmentPreviewApiViewTestCase(ExtendedAPITestCase, ESTestCase):
    SECTIONS = [Sections.BRAND_SAFETY, Sections.SEGMENTS, Sections.STATS]

    def _get_url(self, segment_type, pk):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_PREVIEW,
                       kwargs=dict(segment_type=segment_type, pk=pk))

    def test_segment_channel_segment_preview(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=0,
            segment_type=1,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "channel", str(segment.uuid))
        ChannelManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("channel", segment.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), items)
        self.assertTrue(all(str(segment.uuid) in item["segments"]["uuid"] for item in response.data["items"]))

    def test_segment_video_segment_preview(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=0,
            segment_type=0,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "video", str(segment.uuid))
        VideoManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("video", segment.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), items)
        self.assertTrue(all(str(segment.uuid) in item["segments"]["uuid"] for item in response.data["items"]))

    def test_negative_page(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=0,
            segment_type=0,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "video", str(segment.uuid))
        VideoManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("video", segment.id) + "?page=-1"
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["current_page"], 1)

    def test_negative_size(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=0,
            segment_type=1,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "channel", str(segment.uuid))
        ChannelManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("channel", segment.id) + "?size=-10"
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["current_page"], 1)

    def test_sorted_videos_by_views(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=0,
            segment_type=0,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "video", str(segment.uuid))
        VideoManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("video", segment.id)
        response = self.client.get(url)
        view_counts = [item["stats"]["views"] for item in response.data["items"]]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(sorted(view_counts, reverse=True), view_counts)

    def test_sorted_channels_by_subscribers(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=0,
            segment_type=1,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "channel", str(segment.uuid))
        ChannelManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("channel", segment.id)
        response = self.client.get(url)
        view_counts = [item["stats"]["subscribers"] for item in response.data["items"]]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(sorted(view_counts, reverse=True), view_counts)

    def test_invalid_page(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=0,
            segment_type=1,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "video", str(segment.uuid))
        ChannelManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("channel", segment.id) + "?page=2v"
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_invalid_size(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=0,
            segment_type=1,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "channel", str(segment.uuid))
        VideoManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("video", segment.id) + "?page=9a"
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_channel_segment_not_found(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=1,
            segment_type=1,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "channel", str(segment.uuid))
        ChannelManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("channel", segment.id + 1)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_video_segment_not_found(self):
        self.create_admin_user()
        items = 5
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=next(int_iterator),
            list_type=0,
            segment_type=1,
        )
        mock_data = PersistentSegmentPreviewApiViewTestCase.get_mock_data(items, "video", str(segment.uuid))
        VideoManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("video", segment.id + 1)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
