import json
from io import BytesIO
from unittest.mock import patch

import boto3
from django.conf import settings
from django.utils import timezone
from django.urls import reverse
from moto import mock_s3
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.models import AuditProcessor
from audit_tool.models import AuditChannelVet
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.api.serializers.ctl_serializer import CTLSerializer
from segment.api.serializers.ctl_params_serializer import CTLParamsSerializer
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models import CustomSegmentSourceFileUpload
from segment.models import CustomSegmentVettedFileUpload
from segment.models import SegmentAction
from segment.models.constants import SegmentActionEnum
from segment.models.constants import VideoExclusion
from segment.models.constants import SegmentTypeEnum
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase
from utils.datetime import now_in_default_tz
from utils.unittests.patch_bulk_create import patch_bulk_create


@patch("segment.api.serializers.ctl_serializer.generate_custom_segment")
@patch("segment.models.models.safe_bulk_create", new=patch_bulk_create)
class SegmentCreateUpdateApiViewTestCase(ExtendedAPITestCase, ESTestCase):

    def _get_url(self):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_CREATE)

    @staticmethod
    def get_params(*_, **kwargs):
        params = {
            "ads_stats_include_na": False,
            "age_groups": [],
            "average_cpv": None,
            "average_cpm": None,
            "content_categories": [],
            "content_quality": 1,
            "content_type": 1,
            "countries": [],
            "countries_include_na": False,
            "ctr": None,
            "ctr_v": None,
            "exclude_content_categories": [],
            "exclusion_hit_threshold": None,
            "gender": None,
            "ias_verified_date": "",
            "is_vetted": 1,
            "inclusion_hit_threshold": None,
            "languages": [],
            "languages_include_na": False,
            "last_30day_views": None,
            "last_upload_date": "",
            "min_duration": "",
            "max_duration": "",
            "minimum_videos": None,
            "minimum_videos_include_na": None,
            "minimum_views": None,
            "minimum_views_include_na": None,
            "minimum_subscribers": None,
            "minimum_subscribers_include_na": False,
            "mismatched_language": None,
            "score_threshold": 4,
            "sentiment": 1,
            "severity_filters": None,
            "vetted_after": "",
            "vetting_status": [],
            "video_view_rate": None,
            "video_quartile_100_rate": None,
        }
        params.update(kwargs)
        return params

    def test_reject_permission(self, mock_generate):
        """
        user without create perms should not be allowed to create
        :param mock_generate:
        :return:
        """
        self.create_test_user()
        response = self.client.post(self._get_url(), {})
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(mock_generate.call_count, 0)

    def test_reject_bad_request(self, mock_generate):
        self.create_admin_user()
        data = {
            "list_type": "whitelist",
            "segment_type": 2
        }
        params = self.get_params(**data)
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
        data = self.get_params(**data)
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
        data = self.get_params(**data)
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
        data = self.get_params(**data)
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
        data = self.get_params(**data)
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
        payload = self.get_params(**payload)
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
        payload = self.get_params(**payload)
        form = dict(data=json.dumps(payload))
        with patch("segment.utils.query_builder.SegmentQueryBuilder.map_content_categories", return_value="test_category"):
            response = self.client.post(self._get_url(), form)
        data = response.data
        query = CustomSegmentFileUpload.objects.get(segment_id=data["id"]).query
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(query["params"]["minimum_views"], data["ctl_params"]["minimum_views"])

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
        payload_1 = self.get_params(**payload_1)
        payload_2 = self.get_params(**payload_2)
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
        payload = self.get_params(**payload)
        payload["title"] = "video"
        payload["segment_type"] = 0
        form = dict(data=json.dumps(payload))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertTrue(CustomSegment.objects.filter(
            title=payload["title"], segment_type=payload["segment_type"]
        ).exists())
        mock_generate.delay.assert_called_once()

        with patch("segment.api.serializers.ctl_serializer.generate_custom_segment") as mock_generate:
            payload["title"] = f"test_segment_creation_channel_{next(int_iterator)}"
            payload["segment_type"] = 1
            form = dict(data=json.dumps(payload))
            response = self.client.post(self._get_url(), form)
            self.assertEqual(response.status_code, HTTP_201_CREATED)
            self.assertTrue(CustomSegment.objects.filter(
                title=payload["title"], segment_type=payload["segment_type"]
            ).exists())
            mock_generate.delay.assert_called_once()

    def test_create_handle_source_file_empty_rows(self, mock_generate):
        """ Procesing source urls should handle empty rows """
        self.create_admin_user()
        payload = {
            "title": "test_create_handle_source_file_empty_rows",
            "segment_type": 1,
        }
        payload = self.get_params(**payload)
        file = BytesIO()
        file.write(f"https://www.youtube.com/channel/{str(next(int_iterator)).zfill(24)}".encode("utf-8"))
        file.write(b"\n")
        file.write(b"\n")
        file.name = payload["title"]
        file.seek(0)
        form = dict(
            source_file=file,
            data=json.dumps(payload)
        )
        with patch("segment.models.custom_segment.SegmentExporter"):
            response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)

    def test_create_fail_source_video_invalid_format(self, mock_generate):
        self.create_admin_user()
        payload = {
            "title": "test_create_fail_source_video_invalid_format",
            "segment_type": 0,
        }
        payload = self.get_params(**payload)
        file = BytesIO()
        file.write(f"https://www.youtube.com/v/bad".encode("utf-8"))
        file.name = payload["title"]
        file.seek(0)
        form = dict(
            source_file=file,
            data=json.dumps(payload)
        )
        with patch("segment.models.custom_segment.SegmentExporter"):
            response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_create_fail_source_channel_invalid_format(self, mock_generate):
        self.create_admin_user()
        payload = {
            "title": "test_create_fail_source_channel_invalid_format",
            "segment_type": 0,
        }
        payload = self.get_params(**payload)
        file = BytesIO()
        file.write(f"https://www.youtube.com/chan/bad".encode("utf-8"))
        file.name = payload["title"]
        file.seek(0)
        form = dict(
            source_file=file,
            data=json.dumps(payload)
        )
        with patch("segment.models.custom_segment.SegmentExporter"):
            response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_fail_exclusion_empty(self, mock_generate):
        self.create_admin_user()
        payload = {
            "title": "test exclusion",
            "segment_type": 1,
            "exclusion_hit_threshold": 1,
        }
        exclusion_file = BytesIO()
        exclusion_file.name = "test_exclusion_fail.csv"
        payload = self.get_params(**payload)
        form = dict(
            exclusion_file=exclusion_file,
            data=json.dumps(payload)
        )
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        ctl = CustomSegment.objects.get(title=payload["title"])
        self.assertTrue("empty exclusion" in ctl.statistics["error"])

    def test_fail_inclusion_empty(self, mock_generate):
        self.create_admin_user()
        payload = {
            "title": "test_fail_inclusion_empty",
            "segment_type": 0,
            "inclusion_hit_threshold": 1,
        }
        inclusion_file = BytesIO()
        inclusion_file.name = "test_inclusion.csv"
        payload = self.get_params(**payload)
        form = dict(
            inclusion_file=inclusion_file,
            data=json.dumps(payload)
        )
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        ctl = CustomSegment.objects.get(title=payload["title"])
        self.assertTrue("empty inclusion" in ctl.statistics["error"])

    def test_create_with_source_success(self, mock_generate):
        """ Test creates source with success with at least one valid url"""
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
        payload = self.get_params(**payload)
        file = BytesIO()
        file.write(f"https://www.youtube.com/watch?v={str(next(int_iterator)).zfill(11)}\n".encode("utf-8"))
        file.write("bad_url".encode("utf-8"))
        file.name = payload["title"]
        file.seek(0)
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

    @mock_s3
    def test_create_with_source_limit(self, mock_generate):
        """ Test that source list is limited to size """
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        self.create_admin_user()
        payload = {
            "title": "test_create_with_source_success_limit",
            "segment_type": 1,
        }
        payload = self.get_params(**payload)
        file = BytesIO()
        file.name = payload["title"]
        file.write(b"\n".join([f"https://www.youtube.com/channel/{str(i).zfill(24)}".encode("utf-8")
                               for i in range(300000)]))
        file.seek(0)
        form = dict(
            source_file=file,
            data=json.dumps(payload)
        )
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertTrue(CustomSegment.objects.filter(title=payload["title"]).exists())
        source = CustomSegment.objects.get(id=response.data["id"]).source
        self.assertEqual(file.name, source.name)

        exported_soure_list = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, source.filename)\
            .get()["Body"].read().decode('utf-8').split()
        self.assertEqual(len(exported_soure_list), CTLSerializer.SOURCE_LIST_MAX_SIZE)

    def test_user_not_admin_has_permission_success(self, mock_generate):
        """ User should ctl create permission but is not admin should still be able to create a list """
        self.create_test_user(perms={StaticPermissions.BUILD__CTL_CREATE_VIDEO_LIST: True})
        data = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test blacklist",
            "content_categories": [],
            "minimum_views": 0,
            "segment_type": SegmentTypeEnum.VIDEO.value,
            "content_type": 0,
            "content_quality": 0,
        }
        data = self.get_params(**data)
        form = dict(data=json.dumps(data))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)

        self.create_test_user(perms={StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST: True})
        data["segment_type"] = SegmentTypeEnum.CHANNEL.value
        data = self.get_params(**data)
        form = dict(data=json.dumps(data))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)

    def test_user_not_admin_permission_failure(self, mock_generate):
        """
        test individual video/channel create permissions
        :param mock_generate:
        :return:
        """
        self.create_test_user(perms={StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST: True})
        data = {
            "languages": ["es"],
            "score_threshold": 1,
            "title": "test blacklist",
            "content_categories": [],
            "minimum_views": 0,
            "segment_type": SegmentTypeEnum.VIDEO.value,  # video
            "content_type": 0,
            "content_quality": 0,
        }
        data = self.get_params(**data)
        form = dict(data=json.dumps(data))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

        # test video perm not allowed
        self.create_test_user(perms={StaticPermissions.BUILD__CTL_CREATE_VIDEO_LIST: True})
        data["segment_type"] = SegmentTypeEnum.CHANNEL.value
        data = self.get_params(**data)
        form = dict(data=json.dumps(data))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_segment_saves_params(self, mock_generate):
        """ Test that params saves successfully """
        self.create_admin_user()
        inclusion_file = BytesIO()
        inclusion_file.name = "test_inclusion.csv"
        inclusion_file.write(b"inclusion_word")
        inclusion_file.seek(0)
        exclusion_file = BytesIO()
        exclusion_file.write(b"exclusion_word")
        exclusion_file.seek(0)
        exclusion_file.name = "test_inclusion.csv"
        payload = {
            "title": "test saves params",
            "segment_type": 0
        }
        payload = self.get_params(**payload)
        form = dict(
            inclusion_file=inclusion_file,
            exclusion_file=exclusion_file,
            data=json.dumps(payload)
        )
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)

        segment = CustomSegment.objects.get(id=response.data["id"])
        expected_params = {
            "inclusion_file": inclusion_file.name,
            "exclusion_file": exclusion_file.name,
        }
        saved_params = {
            key: segment.params.get(key, None) for key in expected_params.keys()
        }
        self.assertEqual(expected_params, saved_params)

    def test_inclusion_exclusion_success(self, mock_generate):
        """
        Test that saving with inclusion and exclusion list creates audit.
        Should correctly use first column for keywords
        """
        user = self.create_admin_user()
        payload = {
            "title": "test inclusion",
            "segment_type": 1,
            "inclusion_hit_threshold": 3,
            "exclusion_hit_threshold": 4,
        }
        inclusion_file = BytesIO()
        inclusion_file.name = "test_inclusion.csv"
        in_words = "\n".join(f"include_word_{i}" for i in range(10))
        inclusion_file.write(in_words.encode("utf-8"))
        inclusion_file.seek(0)

        exclusion_file = BytesIO()
        exclusion_file.name = "test_exclusion.csv"
        ex_words = "\n".join(f"exclude_word_{i}, test_exclusion_category" for i in range(10))
        exclusion_file.write(ex_words.encode("utf-8"))
        exclusion_file.seek(0)
        payload = self.get_params(**payload)
        form = dict(
            inclusion_file=inclusion_file,
            exclusion_file=exclusion_file,
            data=json.dumps(payload)
        )
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        audit = AuditProcessor.objects.get(params__segment_id=response.data["id"])
        params = audit.params
        self.assertEqual(audit.name, payload["title"].lower())
        self.assertEqual(audit.source, 2)
        self.assertEqual(params["user_id"], user.id)
        self.assertEqual(params["do_videos"], True)
        self.assertEqual(params["num_videos"], 15)
        self.assertEqual(params["name"], payload["title"])
        self.assertEqual(audit.temp_stop, True)
        self.assertEqual([row.strip() for row in in_words.split("\n")], params["inclusion"])
        self.assertEqual([row.split(",")[0].strip() for row in ex_words.split("\n")], [row[0] for row in params["exclusion"]])
        self.assertEqual(params["inclusion_hit_count"], payload["inclusion_hit_threshold"])
        self.assertEqual(params["exclusion_hit_count"], payload["exclusion_hit_threshold"])
        self.assertEqual(params["files"]["inclusion"], inclusion_file.name)
        self.assertEqual(params["files"]["exclusion"], exclusion_file.name)
        task_args = mock_generate.method_calls
        self.assertEqual(task_args[0][1][0], response.data["id"])
        self.assertEqual(task_args[0][2]["with_audit"], True)

    def test_creates_create_action(self, mock_generate):
        """ Test creating CTL creates CREATE action """
        now = now_in_default_tz()
        user = self.create_admin_user()
        payload = self.get_params(title="test", segment_type=1)
        form = dict(data=json.dumps(payload))
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        action = SegmentAction.objects.get(user=user, action=SegmentActionEnum.CREATE.value)
        self.assertTrue(action.created_at > now)

    def test_regenerates_params_changed(self, mock_generate):
        """ Test that CTL is regenerated if params have changed """
        self.create_admin_user()
        payload = self.get_params(title="test_regenerate_params", segment_type=1)
        response = self.client.post(self._get_url(), dict(data=json.dumps(payload)))
        self.assertEqual(response.status_code, HTTP_201_CREATED)

        created = CustomSegment.objects.get(id=response.data["id"])
        old_params = created.export.query["params"]
        updated_payload = self.get_params(id=created.id, minimum_views=1, segment_type=1)

        with patch("segment.api.serializers.ctl_serializer.generate_custom_segment.delay") as mock_generate:
            response2 = self.client.patch(self._get_url(), dict(data=json.dumps(updated_payload)))
        self.assertEqual(response2.status_code, HTTP_200_OK)

        updated_params = CustomSegment.objects.get(id=created.id).export.query["params"]
        self.assertNotEqual(old_params, updated_params)
        mock_generate.assert_called_once()

    def test_regenerate_exclusion_keywords_changed(self, mock_generate):
        """ Test that CTL is regenerated if exclusion keywords have changed """
        self.create_admin_user()
        payload = self.get_params(title="test_regenerate_keywords", segment_type=0, exclusion_hit_threshold=1)
        exclusion_file = BytesIO()
        exclusion_file.name = "test_exclusion.csv"
        exclusion_file.write(b"a_word")
        exclusion_file.seek(0)
        form = dict(
            data=json.dumps(payload),
            exclusion_file=exclusion_file
        )
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)

        created = CustomSegment.objects.get(id=response.data["id"])
        old_audit = AuditProcessor.objects.get(id=created.params["meta_audit_id"])

        updated_exclusion_file = BytesIO()
        updated_exclusion_file.name = "test_exclusion.csv"
        updated_exclusion_file.write(b"a_changed_word")
        updated_exclusion_file.seek(0)
        updated_payload = self.get_params(id=created.id, segment_type=0, exclusion_hit_threshold=1)
        form2 = dict(
            data=json.dumps(updated_payload),
            exclusion_file=updated_exclusion_file
        )
        with patch("segment.api.serializers.ctl_serializer.generate_custom_segment.delay") as mock_generate:
            response2 = self.client.patch(self._get_url(), form2)
        self.assertEqual(response2.status_code, HTTP_200_OK)

        created.refresh_from_db()
        new_audit = AuditProcessor.objects.get(id=created.params["meta_audit_id"])
        self.assertNotEqual(old_audit.params["exclusion"], new_audit.params["exclusion"])
        self.assertEqual(new_audit.params["segment_id"], created.id)
        mock_generate.assert_called_once()

    def test_regenerate_inclusion_keywords_changed(self, mock_generate):
        """ Test that CTL is regenerated if inclusion keywords have changed """
        self.create_admin_user()
        payload = self.get_params(title="test_regenerate_keywords", segment_type=1)
        inclusion_file = BytesIO()
        inclusion_file.name = "test_inclusion.csv"
        inclusion_file.write(b"an_inclusive_word")
        inclusion_file.seek(0)
        form = dict(
            data=json.dumps(payload),
            inclusion_file=inclusion_file
        )
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)

        created = CustomSegment.objects.get(id=response.data["id"])
        old_audit = AuditProcessor.objects.get(id=created.params["meta_audit_id"])

        updated_inclusion_file = BytesIO()
        updated_inclusion_file.name = "test_new_inclusion.csv"
        updated_inclusion_file.write(b"a_changed_word")
        updated_inclusion_file.seek(0)
        updated_payload = self.get_params(id=created.id, segment_type=payload["segment_type"])
        form2 = dict(
            data=json.dumps(updated_payload),
            inclusion_file=updated_inclusion_file
        )
        with patch("segment.api.serializers.ctl_serializer.generate_custom_segment.delay") as mock_generate:
            response2 = self.client.patch(self._get_url(), form2)
        self.assertEqual(response2.status_code, HTTP_200_OK)

        created.refresh_from_db()
        new_audit = AuditProcessor.objects.get(id=created.params["meta_audit_id"])
        self.assertNotEqual(old_audit.params["inclusion"], new_audit.params["inclusion"])
        self.assertEqual(new_audit.params["segment_id"], created.id)
        mock_generate.assert_called_once()

    def test_does_not_regenerate_same_params(self, mock_generate):
        """ Test CTL export does not regenerate if changing simple column values"""
        self.create_admin_user()
        payload = self.get_params(title="test_no_regenerate", segment_type=1)
        response = self.client.post(self._get_url(), dict(data=json.dumps(payload)))
        self.assertEqual(response.status_code, HTTP_201_CREATED)

        created = CustomSegment.objects.get(id=response.data["id"])
        old_params = created.export.query["params"]

        updated_payload = self.get_params(id=created.id, title="updated_no_regenerate_title", segment_type=1)
        with patch("segment.api.serializers.ctl_serializer.generate_custom_segment.delay") as mock_generate_again:
            response2 = self.client.patch(self._get_url(), dict(data=json.dumps(updated_payload)))
        self.assertEqual(response2.status_code, HTTP_200_OK)
        mock_generate.delay.assert_called_once()

        updated_params = CustomSegment.objects.get(id=created.id).export.query["params"]
        self.assertEqual(old_params, updated_params)
        mock_generate_again.assert_not_called()

    def test_regenerate_source_urls_changed(self, mock_generate):
        """ Test that CTL is regenerated if source urls have changed """
        user = self.create_admin_user()
        payload = self.get_params(segment_type=SegmentTypeEnum.CHANNEL.value)
        params = CTLParamsSerializer(data=payload)
        params.is_valid(raise_exception=True)

        segment = CustomSegment.objects.create(title="test_regenerate_source", owner=user, segment_type=1)
        CustomSegmentFileUpload.objects.create(segment=segment, query=dict(params=params.validated_data))
        CustomSegmentSourceFileUpload.objects.create(segment=segment, filename="old.csv", source_type=0)

        payload.update(dict(id=segment.id, segment_type=segment.segment_type))
        source_file = BytesIO()
        source_file.name = "test_source.csv"
        source_file.write(f"https://www.youtube.com/channel/{str(next(int_iterator)).zfill(24)}\n".encode("utf-8"))
        source_file.write(f"https://www.youtube.com/channel/{str(next(int_iterator)).zfill(24)}".encode("utf-8"))
        source_file.seek(0)
        form = dict(
            data=json.dumps(payload),
            source_file=source_file
        )
        with patch("segment.models.custom_segment.SegmentExporter.get_extract_export_ids",
                   return_value=["source_url"]),\
                patch("segment.models.custom_segment.SegmentExporter.export_file_to_s3"):
            response = self.client.patch(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_200_OK)
        mock_generate.delay.assert_called_once()

    def test_no_regenerate_editing_without_suitability_keywords(self, mock_generate):
        """
        Test that CTL is NOT regenerated if a CTL was created with inclusion / exclusion keywords but
        subsequent editing does not send inclusion_file / exclusion_file in the request. Changes for keywords
        should only be done if inclusion_file / exclusion_file is sent while editing
        """
        user = self.create_admin_user()
        payload = self.get_params(segment_type=1, inclusion_hit_threshold=1)
        params = CTLParamsSerializer(data=payload)
        params.is_valid(raise_exception=True)
        segment = CustomSegment.objects.create(title="test_no_regenerate_source", owner=user,
                                               segment_type=payload["segment_type"])
        CustomSegmentFileUpload.objects.create(segment=segment, query=dict(params=params.validated_data))
        audit_params = dict(
            name=segment.title,
            segment_id=segment.id,
            inclusion=["test_word"],
            exclusion=[],
            files=dict(
                inclusion="inclusion_file",
            )
        )
        audit = AuditProcessor.objects.create(source=2, params=audit_params)
        segment.params.update({"meta_audit_id": audit.id})
        segment.save()
        payload.update(dict(
            id=segment.id, segment_type=segment.segment_type, title="updated_title",
            inclusion_hit_threshold=1
        ))
        # Not sending a inclusion_file in request will not check changes for it
        form = dict(
            data=json.dumps(payload),
        )
        with patch("segment.models.custom_segment.SegmentExporter.get_extract_export_ids", return_value=["an_old_url"]), \
             patch("segment.models.custom_segment.SegmentExporter.export_object_to_s3"):
            response = self.client.patch(self._get_url(), form)
        audit.refresh_from_db()
        segment.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        # Title should change to "updated_title"
        self.assertEqual(segment.title, payload["title"])
        self.assertEqual(audit.params["name"], payload["title"])
        self.assertEqual(audit.name, payload["title"].lower())
        mock_generate.delay.assert_not_called()

        # Test exclusion keywords should not be checked if no file is sent
        audit.params.update({"exclusion": ["some exclusion"]})
        audit.save()
        payload.update({"title": "another changed title"})
        form2 = dict(data=json.dumps(payload))
        with patch("segment.models.custom_segment.SegmentExporter.get_extract_export_ids", return_value=["an_old_url"]), \
             patch("segment.models.custom_segment.SegmentExporter.export_object_to_s3"):
            response = self.client.patch(self._get_url(), form2)
        audit.refresh_from_db()
        segment.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        # Title should change to "another changed title"
        self.assertEqual(segment.title, payload["title"])
        self.assertEqual(audit.params["name"], payload["title"])
        self.assertEqual(audit.name, payload["title"].lower())
        mock_generate.delay.assert_not_called()

    def test_no_regenerate_editing_without_source(self, mock_generate):
        """
        Test that CTL is NOT regenerated if a CTL was created with a source but subsequent editing does not
        send a source file in the request. Changes for source urls should only be done if a source file is sent
        while editing
        """
        user = self.create_admin_user()
        payload = self.get_params(segment_type=1)
        params = CTLParamsSerializer(data=payload)
        params.is_valid(raise_exception=True)

        # Mock create CTL with initial source file
        segment = CustomSegment.objects.create(title="test_no_regenerate_source", owner=user, segment_type=1)
        CustomSegmentFileUpload.objects.create(segment=segment, query=dict(params=params.validated_data))
        CustomSegmentSourceFileUpload.objects.create(segment=segment, filename="old.csv", source_type=0)

        payload.update(dict(id=segment.id, segment_type=segment.segment_type, title="updated_title_no_regenerate"))
        # Not sending a source_file in request will not check changes for it
        form = dict(
            data=json.dumps(payload),
        )
        with patch("segment.models.custom_segment.SegmentExporter.get_extract_export_ids", return_value=["an_old_url"]),\
                patch("segment.models.custom_segment.SegmentExporter.export_object_to_s3"):
            response = self.client.patch(self._get_url(), form)
        segment.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(segment.title, payload["title"])
        mock_generate.delay.assert_not_called()

    def test_regenerate_creates_new_audit(self, mock_generate):
        """
        Test that regenerating CTL creates new audit and stops previous audit. This test causes regeneration
            through updating exclusion_file
        """
        user = self.create_admin_user()
        payload = self.get_params(segment_type=0, exclusion_hit_threshold=1)
        params = CTLParamsSerializer(data=payload)
        params.is_valid(raise_exception=True)

        segment = CustomSegment.objects.create(title="test_regenerate_source", owner=user,
                                               segment_type=payload["segment_type"])
        audit = AuditProcessor.objects.create(source=2, audit_type=1, params=dict(
            segment_id=segment.id,
            exclusion=["outdated", "words"],
            files=dict(exclusion="old_exclusion"),
        ))
        segment.params = dict(meta_audit_id=audit.id)
        segment.save()
        CustomSegmentFileUpload.objects.create(segment=segment, query=dict(params=params.validated_data))
        CustomSegmentSourceFileUpload.objects.create(segment=segment, filename="older.csv", source_type=0)

        updated_exclusion_file = BytesIO()
        updated_exclusion_file.name = "test_exclusion.csv"
        updated_exclusion_file.write(b"an_updated_word")
        updated_exclusion_file.seek(0)
        payload.update(dict(id=segment.id, segment_type=segment.segment_type))
        form = dict(
            data=json.dumps(payload),
            exclusion_file=updated_exclusion_file,
        )
        with patch("segment.models.custom_segment.SegmentExporter.get_extract_export_ids", return_value=["an_older_url"]), \
             patch("segment.models.custom_segment.SegmentExporter.export_object_to_s3"):
            response = self.client.patch(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_200_OK)
        mock_generate.delay.assert_called_once()

        segment.refresh_from_db()
        audit.refresh_from_db()

        # old audit should be paused and completed
        self.assertNotEqual(audit.completed, None)
        self.assertEqual(audit.pause, 0)
        self.assertEqual(audit.params["stopped"], True)

        new_audit = AuditProcessor.objects.get(id=segment.params["meta_audit_id"])
        updated_exclusion_file.seek(0)
        self.assertNotEqual(audit.id, new_audit.id)
        self.assertNotEqual(audit.params, new_audit.params)
        updated_words = [row[0] for row in new_audit.params["exclusion"]]
        self.assertEqual(set([word.decode("utf-8") for word in updated_exclusion_file]), set(updated_words))

    def test_update_partial_success(self, mock_generate):
        """ Test updating with partial update values is successful """
        self.create_admin_user()
        payload = self.get_params(title="test_partial_update", segment_type=0)
        response = self.client.post(self._get_url(), dict(data=json.dumps(payload)))
        self.assertEqual(response.status_code, HTTP_201_CREATED)

        created = CustomSegment.objects.get(id=response.data["id"])
        old_params = created.export.query["params"]

        partial_params = dict(id=created.id, vetting_status=[0,1], segment_type=0)
        with patch("segment.api.serializers.ctl_serializer.generate_custom_segment.delay") as mock_generate:
            response2 = self.client.patch(self._get_url(), dict(data=json.dumps(partial_params)))
        self.assertEqual(response2.status_code, HTTP_200_OK)

        updated_params = CustomSegment.objects.get(id=created.id).export.query["params"]
        self.assertNotEqual(old_params, updated_params)
        self.assertEqual(updated_params["vetting_status"], partial_params["vetting_status"])
        mock_generate.assert_called_once()

    def test_update_title(self, mock_generate):
        """ Test updating title with partial update is successful """
        self.create_admin_user()
        payload = self.get_params(title="test_partial_update", segment_type=0)
        post_response = self.client.post(self._get_url(), dict(data=json.dumps(payload)))
        self.assertEqual(post_response.status_code, HTTP_201_CREATED)

        created = CustomSegment.objects.get(id=post_response.data["id"])

        partial_params = dict(id=created.id, title="new title", segment_type=0)
        with patch("segment.api.serializers.ctl_serializer.generate_custom_segment.delay") as mock_generate:
            patch_response = self.client.patch(self._get_url(), dict(data=json.dumps(partial_params)))
        self.assertEqual(patch_response.status_code, HTTP_200_OK)

        updated = CustomSegment.objects.get(id=created.id)
        self.assertEqual(updated.title, partial_params["title"])
        self.assertNotEqual(created.updated_at, updated.updated_at)

    def test_empty_update_title_validation(self, mock_generate):
        """
        ensure an update where the CTL's name doesn't change does not
        raise a validation error against its own name
        """
        self.create_admin_user()
        payload = self.get_params(title="test_partial_update", segment_type=0)
        post_response = self.client.post(self._get_url(), dict(data=json.dumps(payload)))
        self.assertEqual(post_response.status_code, HTTP_201_CREATED)

        created = CustomSegment.objects.get(id=post_response.data["id"])

        payload.update(dict(id=created.id))
        with patch("segment.api.serializers.ctl_serializer.generate_custom_segment.delay") as mock_generate:
            patch_response = self.client.patch(self._get_url(), dict(data=json.dumps(payload)))
        self.assertNotEqual(patch_response.status_code, HTTP_400_BAD_REQUEST)
        self.assertNotIn("already exists", patch_response.content.decode("utf-8"))
        self.assertEqual(patch_response.status_code, HTTP_200_OK)

    def test_updating_ctl_filters_uses_existing_audit_params(self, mock_generate):
        """
        Test that updating a CTL which causes regeneration should use old data, if possible.
        For example if updating a CTL with inclusion / exclusion keywords, regeneration should use those old keywords
            if none are provided in the update request
        """
        user = self.create_admin_user()
        payload = self.get_params(segment_type=0, inclusion_hit_threshold=1, exclusion_hit_threshold=1)
        params = CTLParamsSerializer(data=payload)
        params.is_valid(raise_exception=True)

        segment = CustomSegment.objects.create(title="test_regenerate_filters_changed", owner=user,
                                               segment_type=payload["segment_type"])
        audit_params = dict(
            segment_id=segment.id,
            inclusion=["inclusion", "word"],
            exclusion=[["an","",""], ["exclusion","",""]],
            files={
                "inclusion": "test_inclusion.csv",
                "exclusion": "test_exclusion.csv",
            }
        )
        audit = AuditProcessor.objects.create(source=2, audit_type=1, params=audit_params)
        segment.params = dict(meta_audit_id=audit.id)
        segment.save()
        CustomSegmentFileUpload.objects.create(segment=segment, query=dict(params=params.validated_data))
        CustomSegmentSourceFileUpload.objects.create(segment=segment, filename="older.csv", source_type=0)

        payload.update(dict(id=segment.id, segment_type=segment.segment_type,
                            content_categories=["Business & Finance"]))
        form = dict(
            data=json.dumps(payload),
        )
        with patch("segment.models.custom_segment.SegmentExporter.export_object_to_s3"):
            response = self.client.patch(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_200_OK)
        mock_generate.delay.assert_called_once()

        segment.refresh_from_db()
        audit.refresh_from_db()

        # A new audit should have been created, but since inclusion / exclusion words were not changed in the update
        # request, the new audit should have same inclusion / exclusion params as old audit
        new_audit = AuditProcessor.objects.get(id=segment.params["meta_audit_id"])
        self.assertEqual(audit.params["stopped"], True)
        self.assertNotEqual(audit.id, new_audit.id)
        self.assertNotEqual(audit.name, new_audit.name)
        self.assertEqual(audit.params["files"], new_audit.params["files"])
        self.assertEqual(audit.params["inclusion"], new_audit.params["inclusion"])
        self.assertEqual(audit.params["exclusion"], new_audit.params["exclusion"])

    def test_updating_ctl_remove_some_suitability_keywords(self, mock_generate):
        """
        Test that updating with some removed suitability keywords should regenerate
        If the existing CTL has suitability keywords but the request sends hit thresholds as None, then it is implied
            the old keywords should not be used
        """
        user = self.create_admin_user()
        payload = self.get_params(segment_type=1, inclusion_hit_threshold=1, exclusion_hit_threshold=1)
        params = CTLParamsSerializer(data=payload)
        params.is_valid(raise_exception=True)

        segment = CustomSegment.objects.create(title="test_regenerate_keywords_removed", owner=user,
                                               segment_type=payload["segment_type"])
        audit_params = dict(
            segment_id=segment.id,
            inclusion=["inclusion", "word"],
            exclusion=["an", "exclusion"],
            files={
                "inclusion": "test_inclusion.csv",
                "exclusion": "test_exclusion.csv",
            }
        )
        audit = AuditProcessor.objects.create(source=2, audit_type=1, params=audit_params)
        segment.params = dict(meta_audit_id=audit.id)
        segment.save()
        CustomSegmentFileUpload.objects.create(segment=segment, query=dict(params=params.validated_data))
        CustomSegmentSourceFileUpload.objects.create(segment=segment, filename="older.csv", source_type=0)

        # inclusion_hit_threshold = None implies we are removing inclusion keywords
        payload.update(dict(id=segment.id, segment_type=segment.segment_type,
                            inclusion_hit_threshold=None, exclusion_hit_threshold=1))
        form = dict(
            data=json.dumps(payload),
        )
        with patch("segment.models.custom_segment.SegmentExporter.export_object_to_s3"):
            response = self.client.patch(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_200_OK)
        mock_generate.delay.assert_called_once()

        segment.refresh_from_db()
        audit.refresh_from_db()

        # A new audit should have been created, and since inclusion_hit_threshold was sent as
        # None, it is considered removed and a new audit should be created without the old inclusion keywords
        new_audit = AuditProcessor.objects.get(id=segment.params["meta_audit_id"])
        self.assertEqual(audit.params["stopped"], True)
        self.assertNotEqual(audit.id, new_audit.id)
        self.assertNotEqual(audit.name, new_audit.name)
        self.assertEqual(audit.params["files"]["exclusion"], new_audit.params["files"]["exclusion"])
        self.assertFalse(new_audit.params["files"].get("inclusion"))
        self.assertFalse(new_audit.params["inclusion"])

    def test_updating_ctl_remove_all_suitability_keywords(self, mock_generate):
        """
        Test that updating with removing all suitability keywords should regenerate without using audits
        Since all suitability keywords are being removed, a new audit should not be created and metadata stored about
        old audits should be removed from CTL
        """
        user = self.create_admin_user()
        payload = self.get_params(segment_type=1, inclusion_hit_threshold=1, exclusion_hit_threshold=1)
        params = CTLParamsSerializer(data=payload)
        params.is_valid(raise_exception=True)

        segment = CustomSegment.objects.create(title="test_regenerate_keywords_removed", owner=user,
                                               segment_type=payload["segment_type"])
        audit_params = dict(
            segment_id=segment.id,
            inclusion=["inclusion", "word"],
            exclusion=["an", "exclusion"],
            files={
                "inclusion": "test_inclusion.csv",
                "exclusion": "test_exclusion.csv",
            }
        )
        audit = AuditProcessor.objects.create(source=2, audit_type=1, params=audit_params)
        segment.params = dict(meta_audit_id=audit.id)
        segment.save()
        CustomSegmentFileUpload.objects.create(segment=segment, query=dict(params=params.validated_data))
        CustomSegmentSourceFileUpload.objects.create(segment=segment, filename="older.csv", source_type=0)

        # inclusion_hit_threshold=None, exclusion_hit_threshold=None implies we are removing all keywords
        payload.update(dict(id=segment.id, segment_type=segment.segment_type,
                            inclusion_hit_threshold=None, exclusion_hit_threshold=None))
        form = dict(
            data=json.dumps(payload),
        )
        with patch("segment.models.custom_segment.SegmentExporter.export_object_to_s3"):
            response = self.client.patch(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_200_OK)

        mock_generate.delay.assert_called_once()
        task_args = mock_generate.method_calls
        self.assertEqual(task_args[0][1][0], response.data["id"])
        # All keywords are being removed, audit is not required
        self.assertFalse(task_args[0][2].get("with_audit"))

        segment.refresh_from_db()
        audit.refresh_from_db()
        self.assertEqual(audit.params["stopped"], True)
        self.assertFalse(segment.params)

    def test_regneration_deletes_records(self, mock_generate):
        """
        Regeneration of a CTL is considered creating a brand new ctl, and all associated records should be
            removed
        """
        now = timezone.now()
        user = self.create_admin_user()
        audit_id = next(int_iterator)
        audit = AuditProcessor.objects.create(id=audit_id, source=1)
        segment = CustomSegment.objects.create(
            title=f"test_regenerate_remove_related",
            segment_type=1, owner=user, audit_id=audit.id,
            statistics={"items_count": 1}, params={"meta_audit_id": None},
            is_vetting_complete=True,
        )
        CustomSegmentFileUpload.objects.create(segment=segment, query={})
        CustomSegmentSourceFileUpload.objects.create(segment=segment, source_type=1)
        CustomSegmentVettedFileUpload.objects.create(segment=segment)
        ids = [f"channel_{next(int_iterator)}" for _ in range(5)]
        for _id in ids:
            audit_item = segment.audit_utils.model.objects.create(channel_id=_id)
            segment.audit_utils.vetting_model.objects.create(channel=audit_item, audit=audit, processed=now)

        updated_payload = self.get_params(id=segment.id, minimum_views=1, segment_type=1)
        with patch.object(CTLSerializer, "_create_export") as mock_create_export:
            response = self.client.patch(self._get_url(), dict(data=json.dumps(updated_payload)))
        self.assertEqual(response.status_code, HTTP_200_OK)
        segment.refresh_from_db()
        mock_create_export.assert_called_once()
        self.assertEqual(segment.is_vetting_complete, False)
        self.assertEqual(segment.statistics, {})
        self.assertFalse(AuditProcessor.objects.filter(id=audit_id).exists())
        self.assertFalse(AuditChannelVet.objects.filter(id=audit_id).exists())
        # Source should still remain as it is required to build new export
        self.assertTrue(hasattr(segment, "source"))
        self.assertFalse(hasattr(segment, "export"))
        self.assertFalse(hasattr(segment, "vetted_export"))

    def test_create_regular_user_vetted_safe_only(self, mock_generate):
        """ Test that if a user is not an admin nor a vetting admin, lists should be created with vetted safe only """
        self.create_test_user(perms={StaticPermissions.BUILD__CTL_CREATE_VIDEO_LIST: True})
        payload = {
            "languages": ["es"],
            "score_threshold": 1,
            "segment_type": SegmentTypeEnum.VIDEO.value,
            "title": "test vetted safe only",
            "vetting_status": [],
        }
        params = self.get_params(**payload)
        response = self.client.post(self._get_url(), dict(data=json.dumps(params)))
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        ctl = CustomSegment.objects.get(title=payload["title"])
        export = ctl.export
        self.assertEqual(export.query["params"]["vetting_status"], [1])

    def test_update_regular_user_vetted_safe_only(self, mock_generate):
        """ Test that if a user is not an admin nor a vetting admin, lists should be updated with vetted safe only """
        user = self.create_test_user(perms={StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST: True})
        segment = CustomSegment.objects.create(
            title=f"test_regenerate_remove_related",
            segment_type=SegmentTypeEnum.CHANNEL.value, owner=user,
            statistics={"items_count": 1}, params={"meta_audit_id": None},
        )
        CustomSegmentFileUpload.objects.create(segment=segment, query={})
        payload = dict(
            id=segment.id,
            languages=["es"],
            vetting_status=[0]
        )
        params = self.get_params(**payload)
        with patch.object(CTLSerializer, "_start_segment_export_task") as mock_start_export:
            response = self.client.patch(self._get_url(), dict(data=json.dumps(params)))
        self.assertEqual(response.status_code, HTTP_200_OK)
        segment.refresh_from_db()
        export = segment.export
        self.assertEqual(export.query["params"]["vetting_status"], [1])

    def test_empty_channel_source_urls_deletes(self, mock_generate):
        """ Test that channel CTL being created during validation without valid source urls is deleted """
        self.create_admin_user()
        payload = {
            "title": "test_empty_channel_source_urls_deletes",
            "segment_type": 1,
        }
        payload = self.get_params(**payload)
        file = BytesIO()
        file.name = payload["title"]
        file.seek(0)
        form = dict(
            source_file=file,
            data=json.dumps(payload)
        )
        with patch("segment.models.custom_segment.SegmentExporter"),\
                patch.object(CustomSegment, "remove_all_from_segment"):
            response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertFalse(CustomSegment.objects.filter(title=payload["title"]).exists())

    def test_empty_video_source_urls_deletes(self, mock_generate):
        """ Test that video CTL being created during validation without valid source urls is deleted """
        self.create_admin_user()
        payload = {
            "title": "test_empty_video_source_urls_deletes",
            "segment_type": 0,
        }
        payload = self.get_params(**payload)
        file = BytesIO()
        file.name = payload["title"]
        file.seek(0)
        form = dict(
            source_file=file,
            data=json.dumps(payload)
        )
        with patch("segment.models.custom_segment.SegmentExporter"),\
                patch.object(CustomSegment, "remove_all_from_segment"):
            response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertFalse(CustomSegment.objects.filter(title=payload["title"]).exists())

    def test_with_video_exclusion(self, mock_generate):
        self.create_admin_user()
        with self.subTest("Test success creating video exclusion with channel ctl"):
            payload = {
                "title": "test_with_video_exclusion_success",
                "segment_type": 1,
                "with_video_exclusion": True,
            }
            payload = self.get_params(**payload)
            form = dict(
                data=json.dumps(payload)
            )
            response = self.client.post(self._get_url(), form)
            self.assertEqual(response.status_code, HTTP_201_CREATED)

        with self.subTest("Test fail creating video exclusion with video ctl"):
            payload = {
                "title": "test_with_video_exclusion_fail",
                "segment_type": 1,
                "with_video_exclusion": True,
            }
            form = dict(
                data=json.dumps(payload)
            )
            response = self.client.post(self._get_url(), form)
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_video_exclusion_permission(self, mock_generate):
        user = self.create_test_user(perms={StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST: True,})
        payload = {
            "title": "test_video_exclusion_permission",
            "segment_type": 1,
            "with_video_exclusion": True,
        }
        payload = self.get_params(**payload)
        form = dict(
            data=json.dumps(payload)
        )
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

        user.perms.update({StaticPermissions.BUILD__CTL_VIDEO_EXCLUSION: True})
        user.save(update_fields=["perms"])
        response = self.client.post(self._get_url(), form)
        self.assertEqual(response.status_code, HTTP_201_CREATED)

    def test_video_exclusion_update(self, mock_generate):
        """ Test updating channel ctl to create video ctl simply creates video ctl and does not update params """
        user = self.create_test_user(perms={
            StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST: True,
            StaticPermissions.BUILD__CTL_VIDEO_EXCLUSION: True,
        })
        params = self.get_params()
        channel_ctl = CustomSegment.objects.create(segment_type=SegmentTypeEnum.CHANNEL.value, owner=user)
        CustomSegmentFileUpload.objects.create(segment=channel_ctl, query={
            "params": params,
        })
        payload = {
            "id": channel_ctl.id,
            "segment_type": 1,
            "with_video_exclusion": True,
        }
        with patch("segment.api.serializers.ctl_serializer.generate_video_exclusion", return_value="testfile.csv") as mock_exclusion_generate:
            response = self.client.patch(self._get_url(), dict(data=json.dumps(payload)))
        self.assertEqual(response.status_code, HTTP_200_OK)
        channel_ctl.refresh_from_db()
        self.assertEqual(channel_ctl.export.query["params"], params)
        self.assertEqual(channel_ctl.params[VideoExclusion.WITH_VIDEO_EXCLUSION], True)
        mock_exclusion_generate.delay.assert_called_once()
