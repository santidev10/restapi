import json
from unittest.mock import MagicMock
from unittest.mock import patch

from django.urls import reverse
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
import uuid

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.api.views.custom_segment.segment_create_v3 import SegmentCreateApiViewV3
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


@patch("segment.api.views.custom_segment.segment_create_v3.generate_custom_segment")
class SegmentCreateApiViewV3TestCase(ExtendedAPITestCase):
    def _get_url(self):
        return reverse(Namespace.SEGMENT_V3 + ":" + Name.SEGMENT_CREATE)

    def test_reject_permission(self, mock_generate):
        self.create_test_user()
        payload = {}
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(mock_generate.call_count, 0)

    def test_reject_bad_request(self, mock_generate):
        self.create_admin_user()
        payload = {
            "list_type": "whitelist",
            "segment_type": 2
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(mock_generate.call_count, 0)

    def test_invalid_date(self, mock_generate):
        self.create_admin_user()
        payload = {
            "languages": ["es"],
            "list_type": "whitelist",
            "score_threshold": 1,
            "title": "test whitelist",
            "content_categories": [],
            "segment_type": 0,
            "last_upload_date": "2000/01/01"
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(mock_generate.call_count, 0)

    def test_reject_invalid_segment_type(self, mock_generate):
        self.create_admin_user()
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test whitelist",
            "content_categories": [],
            "segment_type": 3
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_reject_invalid_date(self, mock_generate):
        self.create_admin_user()
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test whitelist",
            "content_categories": [],
            "segment_type": 0,
            "last_upload_date": "2000/01/01"
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_response_create(self, mock_generate):
        self.create_admin_user()
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test blacklist",
            "content_categories": [],
            "minimum_views": 0,
            "segment_type": 0
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        data = response.data[0]
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(set(data.keys()), set(SegmentCreateApiViewV3.response_fields + ("statistics",)))
        self.assertTrue(data["pending"])

    def test_create_integer_values(self, mock_generate):
        self.create_admin_user()
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "I am a blacklist",
            "content_categories": [],
            "minimum_views": "1,000,000",
            "minimum_views_include_na": False,
            "segment_type": 1
        }
        with patch("brand_safety.utils.BrandSafetyQueryBuilder.map_content_categories", return_value="test_category"):
            response = self.client.post(
                self._get_url(), json.dumps(payload), content_type="application/json"
            )
        data = response.data[0]
        query = CustomSegmentFileUpload.objects.get(segment_id=data["id"]).query
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(query["params"]["minimum_views"], int(payload["minimum_views"].replace(",", "")))

    def test_reject_duplicate_title_create(self, mock_generate):
        self.create_admin_user()
        payload_1 = {
            "languages": ["en"],
            "score_threshold": 1,
            "title": "testing",
            "content_categories": [],
            "minimum_option": 0,
            "segment_type": 0
        }
        payload_2 = {
            "brand_safety_categories": [],
            "languages": ["pt"],
            "score_threshold": 1,
            "title": "testing",
            "content_categories": [],
            "minimum_option": 0,
            "segment_type": 0
        }
        response_1 = self.client.post(self._get_url(), json.dumps(payload_1), content_type="application/json")
        response_2 = self.client.post(self._get_url(), json.dumps(payload_2), content_type="application/json")
        self.assertEqual(response_1.status_code, HTTP_201_CREATED)
        self.assertEqual(response_2.status_code, HTTP_400_BAD_REQUEST)

    def test_segment_creation(self, mock_generate):
        self.create_admin_user()
        payload = {
            "languages": ["pt"],
            "score_threshold": 1,
            "content_categories": [],
            "minimum_option": 0,
        }
        with patch("segment.api.views.custom_segment.segment_create_v3.generate_custom_segment") as mock_generate:
            payload["title"] = "video"
            payload["segment_type"] = 0
            response = self.client.post(self._get_url(), json.dumps(payload), content_type="application/json")
            self.assertEqual(response.status_code, HTTP_201_CREATED)
            self.assertTrue(CustomSegment.objects.filter(
                title=payload["title"], segment_type=payload["segment_type"], list_type=0
            ).exists())
            mock_generate.delay.assert_called_once()

        with patch("segment.api.views.custom_segment.segment_create_v3.generate_custom_segment") as mock_generate:
            payload["title"] = "channel"
            payload["segment_type"] = 1
            response = self.client.post(self._get_url(), json.dumps(payload), content_type="application/json")
            self.assertEqual(response.status_code, HTTP_201_CREATED)
            self.assertTrue(CustomSegment.objects.filter(
                title=payload["title"], segment_type=payload["segment_type"], list_type=0
            ).exists())
            mock_generate.delay.assert_called_once()

        with patch("segment.api.views.custom_segment.segment_create_v3.generate_custom_segment") as mock_generate:
            payload["title"] = "multiple"
            payload["segment_type"] = 2
            response = self.client.post(self._get_url(), json.dumps(payload), content_type="application/json")
            self.assertEqual(response.status_code, HTTP_201_CREATED)
            self.assertEqual(CustomSegment.objects.filter(title=payload["title"], list_type=0).count(), 2)
            self.assertEqual(mock_generate.delay.call_count, 2)

    def test_segment_creation_raises_deletes(self, mock_generate):
        self.create_admin_user()
        payload = {
            "title": "test_raises",
            "score_threshold": 0,
            "content_categories": [
                "20"
            ],
            "languages": [
                "ar"
            ],
            "severity_counts": {
                "1": [1,2,3],
                "4": [1,3],
                "6": [2]
            },
            "segment_type": 2
        }
        segment = CustomSegment.objects.create(
            id=next(int_iterator),
            title=payload["title"],
            list_type=0,
            segment_type=0,
            uuid=uuid.uuid4()
        )
        with patch("segment.api.views.custom_segment.segment_create_v3.SegmentCreateApiViewV3._create") as mock_create,\
                patch("brand_safety.utils.BrandSafetyQueryBuilder.map_content_categories", return_value="test_category"):
            mock_create_success = MagicMock()
            mock_create_success.id = segment.id
            mock_create.side_effect = [mock_create_success, Exception]
            response = self.client.post(self._get_url(), json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertFalse(CustomSegment.objects.filter(title=payload["title"], segment_type__in=[1, 2]).exists())

