import json
import types
from unittest.mock import patch

from django.urls import reverse
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentCreationOptionsApiViewTestCase(ExtendedAPITestCase):
    def _get_url(self):
        return reverse(Namespace.SEGMENT_V3 + ":" + Name.SEGMENT_CREATION_OPTIONS)

    @patch("brand_safety.utils.BrandSafetyQueryBuilder.execute")
    def test_success(self, es_mock):
        self.create_test_user()
        data = types.SimpleNamespace()
        data.hits = types.SimpleNamespace()
        data.took = 5
        data.timed_out = False
        data.hits.total = 602411
        data.max_score = None
        data.hits.hits = []
        es_mock.return_value = data
        payload = {
            "languages": ["es"],
            "segment_type": 0,
            "score_threshold": 1
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data.get("countries"))
        self.assertIsNotNone(response.data.get("items"))
        self.assertIsNotNone(response.data["options"].get("brand_safety_categories"))
        self.assertIsNotNone(response.data["options"].get("content_categories"))
        self.assertIsNotNone(response.data["options"].get("countries"))

    def test_reject_invalid_params(self):
        self.create_test_user()
        payload = {
            "country": "us"
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
