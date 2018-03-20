from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, \
    HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST

from segment.models import SegmentVideo, SegmentRelatedVideo
from utils.utils_tests import ExtendedAPITestCase, SegmentFunctionalityMixin, \
    SingleDBMixin


class VideoListTestCase(
        ExtendedAPITestCase, SegmentFunctionalityMixin, SingleDBMixin):
    def setUp(self):
        self.url = reverse("video_api_urls:video_list")

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

    def test_simple_list_works(self):
        self.create_admin_user()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_video_segment_filter_success(self):
        self.create_admin_user()
        videos_limit = 5
        video_fields = "video_id"
        videos_data = self.obtain_videos_data(video_fields, videos_limit)
        expected_videos_ids = {obj["video_id"] for obj in videos_data["items"]}
        segment = SegmentVideo.objects.create()
        self.create_segment_relations(
            SegmentRelatedVideo, segment, expected_videos_ids)
        url = "{}?video_segment={}".format(self.url, segment.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data["items_count"], len(expected_videos_ids))
        self.assertEqual(
            {obj["id"] for obj in response.data["items"]},
            expected_videos_ids)
