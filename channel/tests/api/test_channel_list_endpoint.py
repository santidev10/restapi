from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, \
    HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST

from saas.utils_tests import ExtendedAPITestCase
from segment.models import SegmentChannel, SegmentRelatedChannel, SegmentVideo, \
    SegmentRelatedVideo
from singledb.connector import SingleDatabaseApiConnector


class ChannelListTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.url = reverse("channel_api_urls:channel_list")

    def test_channel_segment_filter_does_not_exists(self):
        self._create_admin_user()
        url = "{}?channel_segment=1".format(self.url)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_video_segment_filter_does_not_exists(self):
        self._create_admin_user()
        url = "{}?video_segment=1".format(self.url)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_video_segment_and_channel_segment_together_using(self):
        self._create_admin_user()
        url = "{}?video_segment=1&channel_segment=1".format(self.url)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data.keys(), {"error"})

    def test_simple_list_works(self):
        self._create_admin_user()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_channel_segment_filter_success(self):
        self._create_admin_user()
        channels_ids_count = 5
        channels_ids = self._obtain_channels_ids(channels_ids_count)
        segment = SegmentChannel.objects.create()
        self._create_segment_relations(
            SegmentRelatedChannel, segment, channels_ids)
        url = "{}?channel_segment={}".format(self.url, segment.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], channels_ids_count)
        self.assertEqual(
            {obj["id"] for obj in response.data["items"]}, channels_ids)

    def test_video_segment_filter_success(self):
        self._create_admin_user()
        videos_limit = 5
        videos_data = self._obtain_videos_data(videos_limit)
        videos_ids = {obj["video_id"] for obj in videos_data["items"]}
        expected_channels_ids = {
            obj["channel_id"] for obj in videos_data["items"]}
        segment = SegmentVideo.objects.create()
        self._create_segment_relations(
            SegmentRelatedVideo, segment, videos_ids)
        url = "{}?video_segment={}".format(self.url, segment.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data["items_count"], len(expected_channels_ids))
        self.assertEqual(
            {obj["id"] for obj in response.data["items"]},
            expected_channels_ids)

    def _create_admin_user(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()

    def _obtain_channels_ids(self, size=50):
        size = min(size, 50)
        connector = SingleDatabaseApiConnector()
        params = {"fields": "channel_id", "size": size}
        response = connector.get_channel_list(params)
        return {obj["channel_id"] for obj in response["items"]}

    def _obtain_videos_data(self, size=50):
        size = min(size, 50)
        connector = SingleDatabaseApiConnector()
        params = {"fields": "channel_id,video_id", "size": size}
        response = connector.get_video_list(params)
        return response

    def _create_segment_relations(self, relation_model, segment, related_ids):
        for related_id in related_ids:
            relation_model.objects.create(
                segment=segment, related_id=related_id)
