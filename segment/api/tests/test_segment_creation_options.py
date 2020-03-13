import json
import types
from unittest.mock import patch

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from audit_tool.models import AuditCategory
from cache.models import CacheItem
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from utils.unittests.test_case import ExtendedAPITestCase


@patch("brand_safety.utils.BrandSafetyQueryBuilder.execute")
class SegmentCreationOptionsApiViewTestCase(ExtendedAPITestCase):
    def _get_url(self):
        return reverse(Namespace.SEGMENT_V3 + ":" + Name.SEGMENT_CREATION_OPTIONS)

    def test_success(self, es_mock):
        self.create_test_user()
        data = types.SimpleNamespace()
        data.hits = types.SimpleNamespace()
        data.took = 5
        data.timed_out = False
        data.hits.total = types.SimpleNamespace()
        data.hits.total.value = 602411
        data.max_score = None
        data.hits.hits = []
        es_mock.return_value = data
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "segment_type": 2
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data.get("video_items"))
        self.assertIsNotNone(response.data.get("channel_items"))
        self.assertIsNotNone(response.data["options"].get("brand_safety_categories"))
        self.assertIsNotNone(response.data["options"].get("content_categories"))
        self.assertIsNotNone(response.data["options"].get("countries"))

    def test_reject_invalid_params(self, es_mock):
        self.create_test_user()
        payload = {
            "country": "us"
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_unique_content_categories(self, es_mock):
        self.create_test_user()
        content_category = "Test Category"
        AuditCategory.objects.create(
            category=1, category_display_iab=content_category
        )
        AuditCategory.objects.create(
            category=2, category_display_iab=content_category
        )
        response = self.client.post(
            self._get_url(), None, content_type="application/json"
        )
        self.assertEqual(len(response.data["options"]["content_categories"]), 1)


    def test_success_video_items(self, es_mock):
        self.create_test_user()
        data = types.SimpleNamespace()
        data.hits = types.SimpleNamespace()
        data.took = 5
        data.timed_out = False
        data.hits.total = types.SimpleNamespace()
        data.hits.total.value = 100000
        data.max_score = None
        data.hits.hits = []
        es_mock.return_value = data
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "segment_type": 0
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["video_items"], data.hits.total.value)

    def test_success_channel_items(self, es_mock):
        self.create_test_user()
        data = types.SimpleNamespace()
        data.hits = types.SimpleNamespace()
        data.took = 5
        data.timed_out = False
        data.hits.total = types.SimpleNamespace()
        data.hits.total.value = 100000
        data.max_score = None
        data.hits.hits = []
        es_mock.return_value = data
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "segment_type": 1
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["channel_items"], data.hits.total.value)

    def test_success_params_empty(self, es_mock):
        self.create_test_user()
        data = types.SimpleNamespace()
        data.hits = types.SimpleNamespace()
        data.took = 5
        data.timed_out = False
        data.hits.total = types.SimpleNamespace()
        data.hits.total.value = 100000
        data.max_score = None
        data.hits.hits = []
        es_mock.return_value = data
        payload = {
            "score_threshold": None,
            "languages": None,
            "severity_filters": None,
            "countries": None,
            "segment_type": None,
            "sentiment": None,
            "content_categories": None,
            "last_upload_date": None,
        }
        response = self.client.post(
            self._get_url(), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data["options"].get("brand_safety_categories"))
        self.assertIsNotNone(response.data["options"].get("content_categories"))
        self.assertIsNotNone(response.data["options"].get("countries"))

    def test_success_sorted_countries_languages(self, es_mock):
        self.create_test_user()
        cache, _ = CacheItem.objects.get_or_create(key="channel_aggregations")
        cache.value = {
            "general_data.country_code": {"buckets": [
                {"key": "US", "doc_count": 96894},
                {"key": "IN", "doc_count": 33589},
                {"key": "GB", "doc_count": 18372},
            ]},
            "general_data.lang_code": {"buckets": [
                {"key": "en", "doc_count": 344633},
                {"key": "es", "doc_count": 48062},
                {"key": "ar", "doc_count": 29714},
            ]}
        }
        cache.save()
        response = self.client.post(
            self._get_url(), {}, content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        for i, country in enumerate(cache.value["general_data.country_code"]["buckets"]):
            self.assertEqual(response.data["options"]["country_code"][i]["common"], country["key"])
        for i, lang in enumerate(cache.value["general_data.lang_code"]["buckets"]):
            self.assertEqual(response.data["options"]["lang_code"][i]["title"], lang["key"])
