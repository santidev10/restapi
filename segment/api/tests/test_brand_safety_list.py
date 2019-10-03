def test_get_master_lists_below_threshold(self):
    user = self.create_test_user()


import json

from django.urls import reverse
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
import uuid

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentRelated
from segment.models import CustomSegmentFileUpload
from utils.utittests.test_case import ExtendedAPITestCase
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentType
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.api.views.brand_safety.brand_safety_list import PersistentSegmentListApiView


GOOGLE_ADS_STATISTICS = ("cost", "video_views", "clicks", "impressions", "video_clicks", "video_impressions")
STATISTICS_FIELDS_CHANNEL = ("subscribers", "likes", "dislikes", "views", "audited_videos", "items_count")
STATISTICS_FIELDS_VIDEO = ("items_count", "views", "likes", "dislikes")


class PersistentSegmentApiViewTestCase(ExtendedAPITestCase):
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
        threshold = PersistentSegmentListApiView.MINIMUM_ITEMS_COUNT
        details = {
            "items_count": threshold - 1
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
        segment = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(), is_master=False,
            category=PersistentSegmentCategory.WHITELIST,
        )
        segment.details = segment.calculate_statistics()
        segment.save()
        segment.refresh_from_db()
        response = self.client.get(self._get_url("channel"))
        data = response.data["items"][0]
        self.assertEqual(set(data["statistics"].keys()), GOOGLE_ADS_STATISTICS + STATISTICS_FIELDS_CHANNEL)

    def test_video_segment_statistics_fields(self):
        self.create_admin_user()
        segment = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(), is_master=False,
            category=PersistentSegmentCategory.WHITELIST,
        )
        segment.details = segment.calculate_statistics()
        segment.save()
        segment.refresh_from_db()
        response = self.client.get(self._get_url("channel"))
        data = response.data["items"][0]
        self.assertEqual(set(data["statistics"].keys()), GOOGLE_ADS_STATISTICS + STATISTICS_FIELDS_VIDEO)
