from datetime import timedelta
import json
import types
from unittest.mock import patch

from django.urls import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from audit_tool.models import IASHistory
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.languages import LANGUAGES
from brand_safety.models.bad_word import BadWordCategory
from cache.models import CacheItem
from es_components.countries import COUNTRIES
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from utils.unittests.test_case import ExtendedAPITestCase


@patch("segment.utils.query_builder.SegmentQueryBuilder.execute")
class SegmentCreationOptionsApiViewTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.ingestion_1 = IASHistory.objects.create(name="test1.csv", started=timezone.now()-timedelta(days=7),
                                                completed=timezone.now()-timedelta(days=6))
        self.ingestion_2 = IASHistory.objects.create(name="test2.csv", started=timezone.now()-timedelta(minutes=30),
                                                completed=timezone.now())

    def _get_url(self):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_CREATION_OPTIONS)

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
        self.assertEqual(response.data["options"].get("latest_ias"), self.ingestion_2.started)

    def test_that_content_categories_are_iab_categories(self, es_mock):
        self.create_test_user()
        response = self.client.post(self._get_url(), None, content_type="application/json")
        self.assertEqual(
            response.data["options"]["content_categories"],
            AuditUtils.get_iab_categories()
        )

    def test_that_brand_safety_categories_include_only_vettables(self, es_mock):
        self.create_test_user()
        bad_word_unvettable = BadWordCategory.objects.create(name="unvettable", vettable=False)
        bad_word_vettable = BadWordCategory.objects.create(name="vettable", vettable=True)
        response = self.client.post(self._get_url(), None, content_type="application/json")
        brand_safety_categories = response.data["options"]["brand_safety_categories"]
        names = [category["name"] for category in brand_safety_categories]
        self.assertIn(bad_word_vettable.name, names)
        self.assertNotIn(bad_word_unvettable.name, names)

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
            "segment_type": 1,
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
            "vetted_after": None,
            "video_view_rate": None,
            "average_cpv": None,
            "average_cpm": None,
            "ctr_i": None,
            "ctr_v": None,
            "video_completion_100": None,
            "video_views_30_days": None
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
            "general_data.top_lang_code": {"buckets": [
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
        for i, country_code in enumerate(cache.value["general_data.country_code"]["buckets"]):
            self.assertEqual(response.data["options"]["countries"][i]["id"], country_code["key"])
            self.assertEqual(response.data["options"]["countries"][i]["common"], COUNTRIES[country_code["key"]][0])
        for i, lang_code in enumerate(cache.value["general_data.top_lang_code"]["buckets"]):
            self.assertEqual(response.data["options"]["languages"][i]["id"], lang_code["key"])
            self.assertEqual(response.data["options"]["languages"][i]["title"], LANGUAGES[lang_code["key"]])
