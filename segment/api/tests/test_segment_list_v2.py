from django.urls import reverse
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK
import uuid

from saas.urls.namespaces import Namespace
from segment.api.tests.test_brand_safety_list import GOOGLE_ADS_STATISTICS
from segment.api.tests.test_brand_safety_list import STATISTICS_FIELDS_CHANNEL
from segment.api.tests.test_brand_safety_list import STATISTICS_FIELDS_VIDEO
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentRelated
from segment.models import CustomSegmentFileUpload
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentListCreateApiViewV2TestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_LIST,
                       kwargs=dict(segment_type=segment_type))

    def test_owner_filter_list(self):
        user = self.create_test_user()
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1")
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), list_type=0, segment_type=0, title="2")
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        expected_segments_count = 1
        response = self.client.get(self._get_url("video"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_segments_count)

    def test_list_type_filter_list(self):
        user = self.create_test_user()
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1")
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=1, segment_type=0, title="2")
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        expected_segments_count = 1
        query_prams = QueryDict(
            "list_type={}".format("whitelist")).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("video"), query_prams))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), expected_segments_count)

    def test_sort_by_created_list_descending(self):
        user = self.create_test_user()
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1")
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=1, segment_type=0, title="2")
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        query_prams = QueryDict(
            "sort_by={}".format("created_at")).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("video"), query_prams))
        data = response.data
        self.assertEqual(data["items"][0]["id"], seg_2.id)
        self.assertEqual(data["items"][1]["id"], seg_1.id)

    def test_sort_by_created_ascending(self):
        user = self.create_test_user()
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1")
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=1, segment_type=0, title="2")
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        query_prams = QueryDict(
            "ascending=1&sort_by={}".format("created_at")).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("video"), query_prams))
        data = response.data
        self.assertEqual(data["items"][0]["id"], seg_1.id)
        self.assertEqual(data["items"][1]["id"], seg_2.id)

    def test_sort_by_items_descending(self):
        user = self.create_test_user()
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1", statistics={"items_count": 2})
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=1, segment_type=0, title="2", statistics={"items_count": 1})
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        query_prams = QueryDict(
            "sort_by={}".format("items")).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("video"), query_prams))
        data = response.data
        self.assertEqual(data["items"][0]["id"], seg_1.id)
        self.assertEqual(data["items"][1]["id"], seg_2.id)

    def test_sort_by_items_ascending(self):
        user = self.create_test_user()
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1")
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=1, segment_type=0, title="2")
        CustomSegmentRelated.objects.create(
            related_id="test",
            segment=seg_1
        )
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        query_prams = QueryDict(
            "ascending=1&sort_by={}".format("items")).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("video"), query_prams))
        data = response.data
        self.assertEqual(data["items"][0]["id"], seg_2.id)
        self.assertEqual(data["items"][1]["id"], seg_1.id)

    def test_sort_by_title_descending(self):
        user = self.create_test_user()
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="First")
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=1, segment_type=0, title="Second")
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        query_prams = QueryDict(
            "sort_by={}".format("title")).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("video"), query_prams))
        data = response.data
        self.assertEqual(data["items"][0]["id"], seg_2.id)
        self.assertEqual(data["items"][1]["id"], seg_1.id)

    def test_sort_by_title_ascending(self):
        user = self.create_test_user()
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="First")
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=1, segment_type=0, title="Second")
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        query_prams = QueryDict(
            "ascending=1&sort_by={}".format("title")).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("video"), query_prams))
        data = response.data
        self.assertEqual(data["items"][0]["id"], seg_1.id)
        self.assertEqual(data["items"][1]["id"], seg_2.id)

    def test_default_thumbnail_images_list(self):
        user = self.create_test_user()
        segment = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1")
        CustomSegmentFileUpload.objects.create(segment=segment, query={})
        response = self.client.get(self._get_url("video"))
        data = response.data
        self.assertEqual(len(data["items"][0]["statistics"]["top_three_items"]), 3)

    def test_channel_segment_statistics_fields(self):
        user = self.create_test_user()
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=1,
            list_type=0, title="channel", owner=user,
            statistics={
                "items_count": 0,
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
                "average_brand_safety_score": 0
            }
        )
        CustomSegmentFileUpload.objects.create(segment=segment, query={})
        response = self.client.get(self._get_url("channel"))
        data = response.data["items"][0]
        self.assertEqual(set(data["statistics"].keys()), set(GOOGLE_ADS_STATISTICS + STATISTICS_FIELDS_CHANNEL))

    def test_video_segment_statistics_fields(self):
        user = self.create_test_user()
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=0,
            list_type=0, title="video", owner=user,
            statistics={
                "items_count": 0,
                "dislikes": 0,
                "likes": 0,
                "video_view_rate": 0,
                "ctr": 0,
                "ctr_v": 0,
                "average_cpv": 0,
                "average_cpm": 0,
                "views": 0,
                "monthly_views": 0,
                "average_brand_safety_score": 0
            }
        )
        CustomSegmentFileUpload.objects.create(segment=segment, query={})
        response = self.client.get(self._get_url("video"))
        data = response.data["items"][0]
        self.assertEqual(set(data["statistics"].keys()), set(GOOGLE_ADS_STATISTICS + STATISTICS_FIELDS_VIDEO))

