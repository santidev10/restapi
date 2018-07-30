from unittest import skip

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, \
    HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST

from segment.models import SegmentChannel, SegmentRelatedChannel, SegmentVideo, \
    SegmentRelatedVideo
from utils.utils_tests import ExtendedAPITestCase, SegmentFunctionalityMixin, \
    SingleDBMixin


class ChannelListTestCase(
        ExtendedAPITestCase, SegmentFunctionalityMixin, SingleDBMixin):
    def setUp(self):
        self.url = reverse("channel_api_urls:channel_list")

    def test_channel_segment_filter_does_not_exists(self):
        self.create_admin_user()
        url = "{}?channel_segment=1".format(self.url)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_video_segment_filter_does_not_exists(self):
        self.create_admin_user()
        url = "{}?video_segment=1".format(self.url)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_video_segment_and_channel_segment_together_using(self):
        self.create_admin_user()
        url = "{}?video_segment=1&channel_segment=1".format(self.url)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data.keys(), {"error"})

    @skip("Unknown")
    def test_simple_list_works(self):
        self._create_admin_user()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    @skip("Unknown")
    def test_channel_segment_filter_success(self):
        self._create_admin_user()
        channels_ids_count = 5
        channels_ids = self.obtain_channels_ids(channels_ids_count)
        segment = SegmentChannel.objects.create()
        self.create_segment_relations(
            SegmentRelatedChannel, segment, channels_ids)
        url = "{}?channel_segment={}".format(self.url, segment.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], channels_ids_count)
        self.assertEqual(
            {obj["id"] for obj in response.data["items"]}, channels_ids)

    @skip("Unknown")
    def test_video_segment_filter_success(self):
        self._create_admin_user()
        videos_limit = 5
        videos_data = self.obtain_videos_data(size=videos_limit)
        videos_ids = {obj["video_id"] for obj in videos_data["items"]}
        expected_channels_ids = {
            obj["channel_id"] for obj in videos_data["items"]}
        segment = SegmentVideo.objects.create()
        self.create_segment_relations(
            SegmentRelatedVideo, segment, videos_ids)
        url = "{}?video_segment={}".format(self.url, segment.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data["items_count"], len(expected_channels_ids))
        self.assertEqual(
            {obj["id"] for obj in response.data["items"]},
            expected_channels_ids)
