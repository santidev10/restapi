import json

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentListCreateV2ApiViewTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_LIST,
                       kwargs=dict(segment_type=segment_type))

    def test_reject_bad_request(self):
        self.create_test_user()
        payload = {
            "list_type": "whitelist",
        }
        response = self.client.post(
            self._get_url("channel"), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_channel(self):
        self.create_test_user()
        payload = {
            "brand_safety_categories": ["1", "3", "4", "5", "6"],
            "languages": ["es"],
            "list_type": "whitelist",
            "score_threshold": 100,
            "title": "I am a whitelist",
            "youtube_categories": [],
            "minimum_option": 0
        }
        response = self.client.post(
            self._get_url("channel"), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)

    def test_success_video(self):
        self.create_test_user()
        payload = {
            "brand_safety_categories": ["1", "3", "4", "5", "6"],
            "languages": ["es"],
            "list_type": "blacklist",
            "score_threshold": 100,
            "title": "I am a blacklist",
            "youtube_categories": [],
            "minimum_option": 0
        }
        response = self.client.post(
            self._get_url("video"), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)

    def test_owner_filter(self):
        user = self.create_test_user()
        seg_1 = CustomSegment.objects.create(owner=user, list_type=0, segment_type=0, title="1")
        seg_2 = CustomSegment.objects.create(list_type=0, segment_type=0, title="2")
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        expected_segments_count = 1
        response = self.client.get(self._get_url("channel"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_segments_count)

    def test_admin_filter(self):
        user = self.create_admin_user()
        seg_1 = CustomSegment.objects.create(owner=user, list_type=0, segment_type=0, title="1")
        seg_2 = CustomSegment.objects.create(list_type=0, segment_type=0, title="2")
        seg_3 = CustomSegment.objects.create(list_type=0, segment_type=0, title="3")
        CustomSegmentFileUpload.objects.create(segment=seg_1, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_2, query={})
        CustomSegmentFileUpload.objects.create(segment=seg_3, query={})
        expected_segments_count = 3
        response = self.client.get(self._get_url("channel"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_segments_count)

    def test_reject_duplicate_title(self):
        self.create_test_user()
        payload_1 = {
            "brand_safety_categories": [],
            "languages": ["en"],
            "list_type": "blacklist",
            "score_threshold": 1,
            "title": "testing",
            "youtube_categories": []
            "minimum_option": 0
        }
        payload_2 = {
            "brand_safety_categories": [],
            "languages": ["pt"],
            "list_type": "blacklist",
            "score_threshold": 1,
            "title": "testing",
            "youtube_categories": [],
            "minimum_option": 0
        }
        response_1 = self.client.post(self._get_url("video"), json.dumps(payload_1), content_type="application/json")
        response_2 = self.client.post(self._get_url("video"), json.dumps(payload_2), content_type="application/json")
        self.assertEqual(response_1.status_code, HTTP_201_CREATED)
        self.assertEqual(response_2.status_code, HTTP_400_BAD_REQUEST)
