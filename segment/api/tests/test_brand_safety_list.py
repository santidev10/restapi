from django.urls import reverse
import uuid

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.api.views.brand_safety.brand_safety_list import PersistentSegmentListApiView
from utils.utittests.test_case import ExtendedAPITestCase


GOOGLE_ADS_STATISTICS = ("video_view_rate", "ctr", "ctr_v", "average_cpv", "average_cpm")
STATISTICS_FIELDS_CHANNEL = ("subscribers", "likes", "dislikes", "views", "audited_videos", "items_count")
STATISTICS_FIELDS_VIDEO = ("items_count", "views", "likes", "dislikes")


class PersistentSegmentApiViewTestCase(ExtendedAPITestCase):
    THRESHOLD = PersistentSegmentListApiView.MINIMUM_ITEMS_COUNT

    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT + ":" + Name.PERSISTENT_SEGMENT_LIST,
                       kwargs=dict(segment_type=segment_type))

    def test_get_channel_master_list_no_items(self):
        self.create_admin_user()

        PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(), is_master=True,
            category=PersistentSegmentCategory.WHITELIST
        )
        PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(), is_master=True,
            category=PersistentSegmentCategory.BLACKLIST
        )
        response = self.client.get(self._get_url("channel"))
        self.assertTrue(response.data.get("master_blacklist"))
        self.assertTrue(response.data.get("master_whitelist"))

    def test_ignore_non_master_lists_less_than_threshold(self):
        self.create_admin_user()

        details = {
            "items_count": self.THRESHOLD - 1
        }
        PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(), is_master=False, details=details,
            category=PersistentSegmentCategory.WHITELIST,
        )
        PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(), is_master=False, details=details,
            category=PersistentSegmentCategory.WHITELIST
        )
        response = self.client.get(self._get_url("channel"))
        self.assertFalse(response.data["items"])

    def test_channel_segment_statistics_fields(self):
        self.create_admin_user()
        PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(), is_master=False, title="",
            category=PersistentSegmentCategory.WHITELIST,
            details={
                "items_count": self.THRESHOLD,
                "audited_videos": 0,
                "subscribers": 0,
                "dislikes": 0,
                "likes": 0,
                "video_view_rate": 0,
                "ctr": 0,
                "ctr_v": 0,
                "average_cpv": 0,
                "average_cpm": 0,
                "views": 0,
            }
        )
        response = self.client.get(self._get_url("channel"))
        data = response.data["items"][0]
        self.assertEqual(set(data["statistics"].keys()), set(GOOGLE_ADS_STATISTICS + STATISTICS_FIELDS_CHANNEL))

    def test_video_segment_statistics_fields(self):
        self.create_admin_user()
        PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(), is_master=False, title="",
            category=PersistentSegmentCategory.WHITELIST,
            details={
                "items_count": self.THRESHOLD,
                "dislikes": 0,
                "likes": 0,
                "video_view_rate": 0,
                "ctr": 0,
                "ctr_v": 0,
                "average_cpv": 0,
                "average_cpm": 0,
                "views": 0,
            }
        )
        response = self.client.get(self._get_url("video"))
        data = response.data["items"][0]
        self.assertEqual(set(data["statistics"].keys()), set(GOOGLE_ADS_STATISTICS + STATISTICS_FIELDS_VIDEO))
