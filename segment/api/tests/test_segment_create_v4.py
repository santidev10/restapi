import json
import uuid
from io import BytesIO
from unittest.mock import MagicMock
from unittest.mock import patch

from django.urls import reverse
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from userprofile.permissions import Permissions
from userprofile.permissions import PermissionGroupNames
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


@patch("segment.api.serializers.custom_segment_serializer.generate_custom_segment")
class SegmentCreateApiViewTestCase(ExtendedAPITestCase):
    def _get_url(self):
        return reverse(Namespace.SEGMENT_V3 + ":" + Name.SEGMENT_CREATE)

    def _get_params(self, *_, **kwargs):
        params = {
            "severity_filters": None,
            "score_threshold": 4,
            "content_categories": [],
            "languages": [],
            "countries": [],
            "countries_include_na": False,
            "age_groups": [],
            "age_groups_include_na": False,
            "sentiment": 1,
            "gender": None,
            "content_type": -1,
            "content_quality": -1,
            "is_vetted": 1,
            "minimum_videos": None,
            "minimum_videos_include_na": None,
            "minimum_views": None,
            "minimum_views_include_na": None,
            "minimum_subscribers": None,
            "minimum_subscribers_include_na": False,
            "ads_stats_include_na": False,
            "last_upload_date": "",
            "vetted_after": "",
            "mismatched_language": None,
            "video_view_rate": None,
            "average_cpv": None,
            "average_cpm": None,
            "ctr": None,
            "ctr_v": None,
            "video_quartile_100_rate": None,
            "last_30day_views": None,
            "exclude_content_categories": [],
            "ias_verified_date": ""
        }
        params.update(kwargs)
        return params

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
        data = {
            "list_type": "whitelist",
            "segment_type": 2
        }
        params = self._get_params(**data)
        form = dict(data=json.dumps(params))
        response = self.client.post(
            self._get_url(), form
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(mock_generate.call_count, 0)

    def test_invalid_date(self, mock_generate):
        self.create_admin_user()
        data = {
            "languages": ["es"],
            "list_type": "whitelist",
            "score_threshold": 1,
            "title": "test whitelist",
            "content_categories": [],
            "segment_type": 0,
            "last_upload_date": "2000/01/01",
            "content_type": 0,
            "content_quality": 0,
        }
        data = self._get_params(**data)
        form = dict(data=json.dumps(data))
        response = self.client.post(
            self._get_url(), form
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(mock_generate.call_count, 0)

    def test_reject_invalid_segment_type(self, mock_generate):
        self.create_admin_user()
        data = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test whitelist",
            "content_categories": [],
            "segment_type": 3,
            "content_type": 0,
            "content_quality": 0,
        }
        data = self._get_params(**data)
        form = dict(data=json.dumps(data))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_reject_invalid_date(self, mock_generate):
        self.create_admin_user()
        data = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test whitelist",
            "content_categories": [],
            "segment_type": 0,
            "last_upload_date": "2000/01/01",
            "content_type": 0,
            "content_quality": 0,
        }
        data = self._get_params(**data)
        form = dict(data=json.dumps(data))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_reject_bad_iab_categories(self, mock_generate):
        """
        content categories should only accept T2 IAB categories
        """
        self.create_admin_user()
        content_categories = ["asdf", "zxcv", "qwer", "herp", "derp"]
        data = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test whitelist",
            "content_categories": content_categories,
            "segment_type": 0,
            "last_upload_date": "2000-01-01",
            "content_type": 0,
            "content_quality": 0,
        }
        data = self._get_params(**data)
        form = dict(data=json.dumps(data))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        response_json = response.json()['non_field_errors'][0]
        for category in content_categories:
            self.assertIn(category, response_json)

    def test_success_response_create(self, mock_generate):
        self.create_admin_user()
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test blacklist",
            "content_categories": [],
            "minimum_views": 0,
            "segment_type": 0,
            "content_type": 0,
            "content_quality": 0,
            "video_quartile_100_rate": 0,
            "average_cpm": 0,
            "last_30day_views": 0,
            "average_cpv": 0,
            "video_view_rate": 0,
            "ctr_v": 0,
            "ctr": 0,
        }
        payload = self._get_params(**payload)
        form = dict(data=json.dumps(payload))
        response = self.client.post(self._get_url(), form)
        data = response.data
        self.assertEqual(response.status_code, HTTP_201_CREATED)
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
            "segment_type": 1,
            "content_type": 0,
            "content_quality": 0,
        }
        payload = self._get_params(**payload)
        form = dict(data=json.dumps(payload))
        with patch("segment.utils.query_builder.SegmentQueryBuilder.map_content_categories", return_value="test_category"):
            response = self.client.post(self._get_url(), form)
        data = response.data
        query = CustomSegmentFileUpload.objects.get(segment_id=data["id"]).query
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(query["params"]["minimum_views"], data["minimum_views"])

    def test_reject_duplicate_title_create(self, *_):
        self.create_admin_user()
        payload_1 = {
            "languages": ["en"],
            "score_threshold": 1,
            "title": "testing",
            "content_categories": [],
            "minimum_option": 0,
            "segment_type": 0,
            "content_type": 0,
            "content_quality": 0,
        }
        payload_2 = {
            "brand_safety_categories": [],
            "languages": ["pt"],
            "score_threshold": 1,
            "title": "testing",
            "content_categories": [],
            "minimum_option": 0,
            "segment_type": 0,
            "content_type": 0,
            "content_quality": 0,
        }
        payload_1 = self._get_params(**payload_1)
        payload_2 = self._get_params(**payload_2)
        form_1 = dict(data=json.dumps(payload_1))
        form_2 = dict(data=json.dumps(payload_2))
        response_1 = self.client.post(self._get_url(), form_1)
        response_2 = self.client.post(self._get_url(), form_2)
        self.assertEqual(response_1.status_code, HTTP_201_CREATED)
        self.assertEqual(response_2.status_code, HTTP_400_BAD_REQUEST)

    def test_segment_creation(self, mock_generate):
        self.create_admin_user()
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
        payload["title"] = "video"
        payload["segment_type"] = 0
        form = dict(data=json.dumps(payload))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertTrue(CustomSegment.objects.filter(
            title=payload["title"], segment_type=payload["segment_type"]
        ).exists())
        mock_generate.delay.assert_called_once()

        with patch("segment.api.serializers.custom_segment_serializer.generate_custom_segment") as mock_generate:
            payload["title"] = f"test_segment_creation_channel_{next(int_iterator)}"
            payload["segment_type"] = 1
            form = dict(data=json.dumps(payload))
            response = self.client.post(self._get_url(), form)
            self.assertEqual(response.status_code, HTTP_201_CREATED)
            self.assertTrue(CustomSegment.objects.filter(
                title=payload["title"], segment_type=payload["segment_type"]
            ).exists())
            mock_generate.delay.assert_called_once()

    def test_segment_creation_raises_deletes(self, mock_generate):
        self.create_admin_user()
        payload = {
            "title": "test_segment_creation_raises_deletes",
            "score_threshold": 0,
            "content_categories": [
                "20"
            ],
            "languages": [
                "ar"
            ],
            "severity_counts": {
                "1": [1, 2, 3],
                "4": [1, 3],
                "6": [2]
            },
            "segment_type": 2,
            "content_type": 0,
            "content_quality": 0,
        }
        payload = self._get_params(**payload)
        form = dict(data=json.dumps(payload))
        segment = CustomSegment.objects.create(
            id=next(int_iterator),
            title=payload["title"],
            list_type=0,
            segment_type=0,
            uuid=uuid.uuid4()
        )
        with patch("segment.api.views.custom_segment.segment_create_v3.SegmentCreateApiViewV3._create") as \
            mock_create, \
            patch("segment.utils.query_builder.SegmentQueryBuilder.map_content_categories", return_value="test_category"):
            mock_create_success = MagicMock()
            mock_create_success.id = segment.id
            mock_create.side_effect = [mock_create_success, Exception]
            response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertFalse(CustomSegment.objects.filter(title=payload["title"], segment_type__in=[1, 2]).exists())

    def test_source_one_list_fail(self, mock_generate):
        """ Test only allowing source list with one list creation """
        self.create_admin_user()
        payload = {
            "title": "test_source_one_list_fail",
            "score_threshold": 0,
            "content_categories": [],
            "languages": [],
            "severity_counts": {},
            "segment_type": 2,
            "content_type": 0,
            "content_quality": 0,
        }
        payload = self._get_params(**payload)
        file = BytesIO()
        form = dict(
            file=file,
            data=json.dumps(payload)
        )
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_create_with_source_success(self, mock_generate):
        self.create_admin_user()
        payload = {
            "title": "test_create_with_source_success",
            "score_threshold": 0,
            "content_categories": [],
            "languages": [],
            "severity_counts": {},
            "segment_type": 0,
            "content_type": 0,
            "content_quality": 0,
        }
        payload = self._get_params(**payload)
        file = BytesIO()
        file.name = payload["title"]
        form = dict(
            source_file=file,
            data=json.dumps(payload)
        )
        with patch("segment.models.custom_segment.SegmentExporter"):
            response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertTrue(CustomSegment.objects.filter(title=payload["title"]).exists())
        source = CustomSegment.objects.get(id=response.data["id"]).source
        self.assertEqual(file.name, source.name)

    def test_user_not_admin_has_permission_success(self, mock_generate):
        """ User should ctl create permission but is not admin should still be able to create a list """
        user = self.create_test_user()
        Permissions.sync_groups()
        user.add_custom_user_group(PermissionGroupNames.CUSTOM_TARGET_LIST_CREATION)
        data = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test blacklist",
            "content_categories": [],
            "minimum_views": 0,
            "segment_type": 0,
            "content_type": 0,
            "content_quality": 0,
        }
        data = self._get_params(**data)
        form = dict(data=json.dumps(data))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
