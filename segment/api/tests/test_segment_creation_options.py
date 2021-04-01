from datetime import timedelta
import json
import types
from unittest.mock import patch

from django.urls import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.models import IASHistory
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.languages import LANGUAGES
from brand_safety.models.bad_word import BadWordCategory
from cache.models import CacheItem
from es_components.countries import COUNTRIES
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import ParamsTemplate
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase
from .test_segment_create_update import SegmentCreateUpdateApiViewTestCase


@patch("segment.utils.query_builder.SegmentQueryBuilder.execute")
class SegmentCreationOptionsApiViewTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.ingestion_1 = IASHistory.objects.create(name="test1.csv", started=timezone.now()-timedelta(days=7),
                                                completed=timezone.now()-timedelta(days=6))
        self.ingestion_2 = IASHistory.objects.create(name="test2.csv", started=timezone.now()-timedelta(minutes=30),
                                                completed=timezone.now())

    def _get_url(self):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_CREATION_OPTIONS)

    def _get_params(self, *_, **kwargs):
        params = SegmentCreateUpdateApiViewTestCase.get_params()
        params.update(kwargs)
        return params

    def _get_mock_data(self, hits_total=0, hits=None):
        hits = hits or []
        data = types.SimpleNamespace()
        data.hits = types.SimpleNamespace()
        data.took = 1
        data.timed_out = False
        data.hits.total = types.SimpleNamespace()
        data.hits.total.value = hits_total
        data.max_score = None
        data.hits.hits = hits
        return data

    def test_video_items_success(self, es_mock):
        """ Test estimate count retrieved for video """
        self.create_test_user()
        data = self._get_mock_data(hits_total=602411)
        es_mock.return_value = data
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "segment_type": 0,
            "get_estimate": True,
        }
        payload = self._get_params(**payload)
        response = self.client.generic(method="POST", path=self._get_url(),
                                       data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["video_items"], data.hits.total.value)

    def test_channel_items_success(self, es_mock):
        """ Test estimate count retrieved for channel """
        self.create_test_user()
        data = self._get_mock_data(hits_total=33345)
        es_mock.return_value = data
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "segment_type": 1,
            "get_estimate": True,
        }
        payload = self._get_params(**payload)
        response = self.client.generic(method="POST", path=self._get_url(),
                                       data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["channel_items"], data.hits.total.value)

    def test_that_content_categories_are_iab_categories(self, es_mock):
        self.create_test_user()
        response = self.client.get(self._get_url(), None)
        self.assertEqual(
            response.data["options"]["content_categories"],
            AuditUtils.get_iab_categories()
        )

    def test_that_brand_safety_categories_include_only_vettables(self, es_mock):
        self.create_test_user()
        bad_word_unvettable = BadWordCategory.objects.create(name="unvettable", vettable=False)
        bad_word_vettable = BadWordCategory.objects.create(name="vettable", vettable=True)
        response = self.client.get(self._get_url(), None)
        brand_safety_categories = response.data["options"]["brand_safety_categories"]
        names = [category["name"] for category in brand_safety_categories]
        self.assertIn(bad_word_vettable.name, names)
        self.assertNotIn(bad_word_unvettable.name, names)

    def test_success_params_empty(self, es_mock):
        self.create_test_user()
        payload = self._get_params()
        response = self.client.generic(method="GET", path=self._get_url(),
                                       data=json.dumps(payload), content_type="application/json")
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
        response = self.client.get(self._get_url(), {})
        self.assertEqual(response.status_code, HTTP_200_OK)
        for i, country_code in enumerate(cache.value["general_data.country_code"]["buckets"]):
            self.assertEqual(response.data["options"]["countries"][i]["id"], country_code["key"])
            self.assertEqual(response.data["options"]["countries"][i]["common"], COUNTRIES[country_code["key"]][0])
        for i, lang_code in enumerate(cache.value["general_data.top_lang_code"]["buckets"]):
            self.assertEqual(response.data["options"]["languages"][i]["id"], lang_code["key"])
            self.assertEqual(response.data["options"]["languages"][i]["title"], LANGUAGES[lang_code["key"]])

    def test_update_regular_user_vetted_safe_only(self, es_mock):
        """ Test user gets vetted safe only filter if does not have permission for any vetting status """
        self.create_test_user()
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "segment_type": 1,
            "get_estimate": True,
        }
        payload = self._get_params(**payload)
        with patch("segment.api.views.custom_segment.segment_create_options.SegmentQueryBuilder") as mock_query_builder:
            mock_query_builder.return_value.execute.return_value = self._get_mock_data()
            response = self.client.generic(method="POST", path=self._get_url(), data=json.dumps(payload),
                                           content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        params = mock_query_builder.call_args[0][0]
        self.assertEqual(params["vetting_status"], [1])

    def test_get_param_templates(self, es_mock):
        """ test retrieving list of channel/video param templates if user has permission """
        user = self.create_admin_user()
        params = self._get_params()
        # create channel params template
        channel_template = ParamsTemplate.objects.create(
            title="Test",
            owner=user,
            title_hash=0,
            params=params,
            segment_type=1
        )
        channel_template.save()
        # create video params template
        video_template = ParamsTemplate.objects.create(
            title="Test",
            owner=user,
            title_hash=0,
            params=params,
            segment_type=0
        )
        video_template.save()
        response = self.client.generic(method="GET", path=self._get_url(),
                                       data=json.dumps(params), content_type="application/json")
        self.assertEqual(response.data["channel_templates"][0]["params"], channel_template.params)
        self.assertEqual(response.data["video_templates"][0]["params"], video_template.params)

    def test_delete_params_template(self, es_mock):
        """ tests parameter templates delete api """
        user = self.create_admin_user()
        video_template = ParamsTemplate.objects.create(
            title="Test",
            owner=user,
            segment_type=0
        )
        video_template.save()
        data = {"template_id": video_template.id}
        response = self.client.generic(method="DELETE", path=self._get_url(), data=json.dumps(data),
                                       content_type="application/json")
        self.assertFalse(ParamsTemplate.objects.filter(id=video_template.id).exists())
        self.assertEqual(HTTP_200_OK, response.status_code)

    def test_params_template_create(self, mock_generate):
        """ Tests params template create api """
        user = self.create_admin_user()
        payload = {
            "languages": ["pt"],
            "score_threshold": 1,
            "content_categories": [],
            "minimum_option": 0,
            "vetted_after": "2020-01-01",
            "content_type": 0,
            "content_quality": 0,
        }
        payload = self._get_params(**payload)
        payload["template_title"] = "video template"
        payload["segment_type"] = 0
        response = self.client.generic(method="POST", path=self._get_url(), data=json.dumps(payload),
                                       content_type="application/json")
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertTrue(ParamsTemplate.objects.filter(
            title=payload["template_title"], segment_type=payload["segment_type"], owner=user
        ).exists())

        payload["template_title"] = "channel template"
        payload["segment_type"] = 1
        response = self.client.generic(method="POST", path=self._get_url(), data=json.dumps(payload),
                                   content_type="application/json")
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertTrue(ParamsTemplate.objects.filter(
            title=payload["template_title"], segment_type=payload["segment_type"], owner=user
        ).exists())

    def test_params_template_update(self, mock_generate):
        """ Tests params template update api """
        user = self.create_admin_user()
        payload = {
            "languages": ["pt"],
            "score_threshold": 1,
            "content_categories": [],
            "minimum_option": 0,
            "vetted_after": "2020-01-01",
            "content_type": 0,
            "content_quality": 0,
        }
        payload = self._get_params(**payload)
        payload["template_title"] = "video template"
        payload["segment_type"] = 0
        response = self.client.generic(method="POST", path=self._get_url(), data=json.dumps(payload),
                                       content_type="application/json")
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertTrue(ParamsTemplate.objects.filter(
            title=payload["template_title"], segment_type=payload["segment_type"], owner=user
        ).exists())
        template_object = ParamsTemplate.objects.get(
            title=payload["template_title"], segment_type=payload["segment_type"], owner=user
        )
        payload["content_type"] = 1
        payload["template_id"] = template_object.id
        response = self.client.generic(method="PATCH", path=self._get_url(), data=json.dumps(payload),
                                   content_type="application/json")
        template_object = ParamsTemplate.objects.get(
            title=payload["template_title"], segment_type=payload["segment_type"], owner=user
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(template_object.params["content_type"][0], 1)


    def test_params_template_perm(self, mock_generate):
        """ Tests parameter templates create permission for both channels and videos """
        user = self.create_test_user()
        user.perms[StaticPermissions.BUILD__CTL_PARAMS_TEMPLATE] = False
        user.save()
        payload = {
            "languages": ["pt"],
            "score_threshold": 1,
            "content_categories": [],
            "minimum_option": 0,
            "vetted_after": "2020-01-01",
            "content_type": 0,
            "content_quality": 0,
        }
        payload = self._get_params(**payload)
        payload["template_title"] = "video template"
        payload["segment_type"] = 0
        response = self.client.generic(method="POST", path=self._get_url(), data=json.dumps(payload),
                                   content_type="application/json")
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

        payload["template_title"] = "channel template"
        payload["segment_type"] = 1
        response = self.client.generic(method="POST", path=self._get_url(), data=json.dumps(payload),
                                   content_type="application/json")
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_recreate_params_template_title_type_owner(self, mock_generate):
        """
        Tests recreating a ParamsTemplate instance with same title, segment_type, and owner.
        Should return 400 status code error.
        """
        user = self.create_admin_user()
        video_template = ParamsTemplate.objects.create(
            title="Test",
            owner=user,
            segment_type=0
        )
        video_template.save()
        payload = {
            "languages": ["pt"],
            "score_threshold": 1,
            "content_categories": [],
            "minimum_option": 0,
            "vetted_after": "2020-01-01",
            "content_type": 0,
            "content_quality": 0,
        }
        payload = self._get_params(**payload)
        payload["template_title"] = "Test"
        payload["segment_type"] = 0
        response = self.client.generic(method="POST", path=self._get_url(), data=json.dumps(payload),
                                       content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
