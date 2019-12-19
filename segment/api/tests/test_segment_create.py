import json
from unittest.mock import patch

from django.urls import reverse
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


class SegmentListCreateApiViewV2TestCase(ExtendedAPITestCase):
    def _get_url(self):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_LIST)

    def test_reject_bad_request(self):
        self.create_test_user()
        payload = {
            "list_type": "whitelist",
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_reject_invalid_segment_type(self):
        self.create_test_user()
        payload = {
            "brand_safety_categories": ["1", "3", "4", "5", "6"],
            "languages": ["es"],
            "list_type": "whitelist",
            "score_threshold": 100,
            "title": "I am a whitelist",
            "youtube_categories": [],
            "minimum_option": 0,
            "segment_type": 3
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_response_create(self):
        response_fields = ["id", "list_type", "segment_type", "statistics", "title", "download_url", "pending", "updated_at", "created_at"]
        self.create_test_user()
        payload = {
            "brand_safety_categories": ["1", "3", "4", "5", "6"],
            "languages": ["es"],
            "list_type": "blacklist",
            "score_threshold": 100,
            "title": "I am a blacklist",
            "youtube_categories": [],
            "minimum_option": 0,
            "segment_type": 0
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        data = response.data
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(set(data.keys()), set(response_fields))
        self.assertTrue(data["pending"])

    def test_create_integer_values(self):
        self.create_test_user()
        payload = {
            "brand_safety_categories": ["1", "3", "4", "5", "6"],
            "languages": ["es"],
            "list_type": "blacklist",
            "score_threshold": 100,
            "title": "I am a blacklist",
            "youtube_categories": [],
            "minimum_option": "1,000,000"
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        data = response.data
        query = CustomSegmentFileUpload.objects.get(segment_id=data["id"]).query
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(query["params"]["minimum_views"], 1000000)

    def test_reject_duplicate_title_create(self):
        self.create_test_user()
        payload_1 = {
            "brand_safety_categories": [],
            "languages": ["en"],
            "list_type": "blacklist",
            "score_threshold": 1,
            "title": "testing",
            "youtube_categories": [],
            "minimum_option": 0,
            "segment_type": 0
        }
        payload_2 = {
            "brand_safety_categories": [],
            "languages": ["pt"],
            "list_type": "blacklist",
            "score_threshold": 1,
            "title": "testing",
            "youtube_categories": [],
            "minimum_option": 0,
            "segment_type": 0
        }
        response_1 = self.client.post(self._get_url(), json.dumps(payload_1), content_type="application/json")
        response_2 = self.client.post(self._get_url(), json.dumps(payload_2), content_type="application/json")
        self.assertEqual(response_1.status_code, HTTP_201_CREATED)
        self.assertEqual(response_2.status_code, HTTP_400_BAD_REQUEST)

    def test_segment_creation(self):
        self.create_test_user()
        payload = {
            "brand_safety_categories": [],
            "languages": ["pt"],
            "list_type": "blacklist",
            "score_threshold": 1,
            "youtube_categories": [],
            "minimum_option": 0,
        }
        with patch("segment.api.custom_segment.segment_create_v3.generate_custom_segment") as mock_generate:
            payload["title"] = "video"
            payload["segment_type"] = 0
            response = self.client.post(self._get_url(), json.dumps(payload), content_type="application/json")
            mock_generate.assert_called_once()
            self.assertEqual(response.status_code, HTTP_201_CREATED)
            self.assertTrue(CustomSegment.objects.filter(title=payload["title"], segment_type=payload["segment_type"]).exists())

        with patch("segment.api.custom_segment.segment_create_v3.generate_custom_segment") as mock_generate:
            payload["title"] = "channel"
            payload["segment_type"] = 1
            response = self.client.post(self._get_url(), json.dumps(payload), content_type="application/json")
            mock_generate.assert_called_once()
            self.assertEqual(response.status_code, HTTP_201_CREATED)
            self.assertTrue(CustomSegment.objects.filter(title=payload["title"], segment_type=payload["segment_type"]).exists())

        with patch("segment.api.custom_segment.segment_create_v3.generate_custom_segment") as mock_generate:
            payload["title"] = "multiple"
            payload["segment_type"] = 2
            response = self.client.post(self._get_url(), json.dumps(payload), content_type="application/json")
            mock_generate.assert_called_twice()
            self.assertEqual(response.status_code, HTTP_201_CREATED)
            self.assertTrue(CustomSegment.objects.filter(title=payload["title"], segment_type=payload["segment_type"]).exists())
