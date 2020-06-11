from django.urls import reverse
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.api.views.brand_safety.brand_safety_list import MINIMUM_ITEMS_COUNT
from segment.models import CustomSegment
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent.constants import PersistentSegmentCategory
from utils.unittests.test_case import ExtendedAPITestCase
import uuid


GOOGLE_ADS_STATISTICS = ("video_view_rate", "ctr", "ctr_v", "average_cpv", "average_cpm")
STATISTICS_FIELDS_CHANNEL = ("subscribers", "likes", "dislikes", "views", "audited_videos", "items_count", "monthly_views", "monthly_subscribers", "average_brand_safety_score")
STATISTICS_FIELDS_VIDEO = ("items_count", "views", "likes", "dislikes", "monthly_views", "average_brand_safety_score")


class PersistentSegmentApiViewTestCase(ExtendedAPITestCase):
    THRESHOLD = MINIMUM_ITEMS_COUNT

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

        CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            statistics={"items_count": self.THRESHOLD - 1},
            title="should not appear in response",
            is_featured=True,
            segment_type=1,
        )
        CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            statistics={"items_count": self.THRESHOLD},
            title="SHOULD appear in response",
            is_featured=True,
            segment_type=1,
        )
        response = self.client.get(self._get_url("channel"))
        self.assertEqual(len(response.data["items"]), 1)

    def test_obeys_segment_type(self):
        self.create_admin_user()
        CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            statistics={"items_count": self.THRESHOLD},
            title="should not appear in response",
            is_featured=True,
            segment_type=1,
        )
        CustomSegment.objects.create(
            uuid=uuid.uuid4(),
            statistics={"items_count": self.THRESHOLD},
            title="SHOULD appear in response",
            is_featured=True,
            segment_type=0,
        )
        response = self.client.get(self._get_url("channel"))
        self.assertEqual(len(response.data["items"]), 1)

    def test_custom_segment_statistics_fields(self):
        self.create_admin_user()
        CustomSegment.objects.create(
            segment_type=1,
            uuid=uuid.uuid4(),
            title="test custom segment statistics field",
            is_featured=True,
            statistics={
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
                "monthly_views": 0,
                "monthly_subscribers": 0,
                "average_brand_safety_score": 0,
            }
        )
        response = self.client.get(self._get_url("channel"))
        data = response.data["items"][0]
        self.assertEqual(set(data["statistics"].keys()), set(GOOGLE_ADS_STATISTICS + STATISTICS_FIELDS_CHANNEL))
