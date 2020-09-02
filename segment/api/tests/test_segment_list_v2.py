import uuid

from django.contrib.auth import get_user_model
from django.http import QueryDict
from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from audit_tool.models import AuditProcessor
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from saas.urls.namespaces import Namespace
from segment.api.tests.test_brand_safety_list import GOOGLE_ADS_STATISTICS
from segment.api.tests.test_brand_safety_list import STATISTICS_FIELDS_CHANNEL
from segment.api.tests.test_brand_safety_list import STATISTICS_FIELDS_VIDEO
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models import CustomSegmentRelated
from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class SegmentListCreateApiViewV2TestCase(ExtendedAPITestCase):

    @classmethod
    def setUpClass(cls):
        Permissions.sync_groups()

    @classmethod
    def tearDownClass(cls):
        CustomSegment.objects.all().delete()

    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_LIST,
                       kwargs=dict(segment_type=segment_type))

    def _create_segment(self, segment_params=None, export_params=None):
        segment_params = segment_params if segment_params else {}
        export_params = export_params if export_params else dict(query={})
        if "uuid" not in segment_params:
            segment_params["uuid"] = uuid.uuid4()
        segment = CustomSegment.objects.create(**segment_params)
        export = CustomSegmentFileUpload.objects.create(segment=segment, **export_params)
        return segment, export

    def _create_user(self, user_data=None):
        user_data = user_data if user_data else dict(email=f"test{next(int_iterator)}@test.com")
        user = get_user_model().objects.create(**user_data)
        return user

    def test_owner_filter_list(self):
        user = self.create_test_user()
        seg_1_params = dict(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1")
        seg_2_params = dict(uuid=uuid.uuid4(), list_type=0, segment_type=0, title="2")
        self._create_segment(segment_params=seg_1_params, export_params=dict(query={}))
        self._create_segment(segment_params=seg_2_params, export_params=dict(query={}))
        expected_segments_count = 1
        response = self.client.get(self._get_url("video"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_segments_count)
        self.assertEqual(response.data["items"][0]["owner_id"], str(seg_1_params["owner"].id))

    def test_owner_filter_list_vetted(self):
        """ Users should be able to see and download their own lists, even if vetted """
        user = self.create_test_user()
        audit = AuditProcessor.objects.create(source=0)
        seg_1_params = dict(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1", audit_id=audit.id)
        seg_2_params = dict(uuid=uuid.uuid4(), list_type=0, segment_type=0, title="2")
        segment, export = self._create_segment(segment_params=seg_1_params,
                                               export_params=dict(query={}, download_url="test"))
        self._create_segment(segment_params=seg_2_params, export_params=dict(query={}))
        expected_segments_count = 1
        response = self.client.get(self._get_url("video"))
        data = response.data["items"][0]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_segments_count)
        self.assertEqual(data["owner_id"], str(seg_1_params["owner"].id))
        self.assertEqual(data["download_url"], export.download_url)

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
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1",
                                             statistics={"items_count": 2})
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=1, segment_type=0, title="2",
                                             statistics={"items_count": 1})
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
        seg_1 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1",
                                             statistics={"items_count": 2})
        seg_2 = CustomSegment.objects.create(uuid=uuid.uuid4(), owner=user, list_type=1, segment_type=0, title="2",
                                             statistics={"items_count": 1})
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
                "average_brand_safety_score": 0,
                "sentiment": 0
            }
        )
        CustomSegmentFileUpload.objects.create(segment=segment, query={})
        response = self.client.get(self._get_url("video"))
        data = response.data["items"][0]
        self.assertEqual(set(data["statistics"].keys()), set(GOOGLE_ADS_STATISTICS + STATISTICS_FIELDS_VIDEO))

    def test_channel_fields(self):
        """
        test that certain channel fields are present
        """
        user = self.create_test_user()
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=1,
            list_type=0, title="channel", owner=user,
            statistics={}
        )
        CustomSegmentFileUpload.objects.create(segment=segment, query={})
        response = self.client.get(self._get_url("channel"))
        data = response.data["items"][0]
        assert_equal_map = {
            'segment_type': '1',
        }
        for key, value in assert_equal_map.items():
            with self.subTest(key):
                self.assertEqual(data[key], value)

    def test_video_fields(self):
        """
        test that certain video fields are present
        """
        user = self.create_test_user()
        segment = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=0,
            list_type=0, title="video", owner=user,
        )
        CustomSegmentFileUpload.objects.create(segment=segment, query={})
        response = self.client.get(self._get_url("video"))
        data = response.data["items"][0]
        assert_equal_map = {
            'segment_type': '0',
        }
        for key, value in assert_equal_map.items():
            with self.subTest(key):
                self.assertEqual(data[key], value)

    def test_content_category_filters(self):
        user = self.create_test_user()
        params = {
            "params": {
                "content_categories": [],
            }
        }
        s1 = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=0,
            list_type=0, title="video", owner=user,
        )
        s1_params = params.copy()
        s1_params["params"]["content_categories"] = ["Travel", "Television"]
        CustomSegmentFileUpload.objects.create(segment=s1, query=s1_params)

        s2 = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=0,
            list_type=0, title="video", owner=user,
        )
        s2_params = params.copy()
        s2_params["params"]["content_categories"] = ["Movies"]
        CustomSegmentFileUpload.objects.create(segment=s2, query=s2_params)

        s3 = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=0,
            list_type=0, title="video", owner=user,
        )
        s3_params = params.copy()
        s3_params["params"]["content_categories"] = ["Comedy", "People & Blogs"]
        CustomSegmentFileUpload.objects.create(segment=s3, query=s3_params)

        query_params = QueryDict("general_data.iab_categories=Travel,Movies,Comedy").urlencode()
        response = self.client.get(f"{self._get_url('video')}?{query_params}")
        self.assertEqual({s1.id, s2.id, s3.id}, {int(item["id"]) for item in response.data["items"]})

    def test_language_filters(self):
        user = self.create_test_user()
        params = {
            "params": {
                "languages": [],
            }
        }
        s1 = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=1,
            list_type=0, title="channel", owner=user,
        )
        s1_params = params.copy()
        s1_params["params"]["languages"] = ["es", "en"]
        CustomSegmentFileUpload.objects.create(segment=s1, query=s1_params)

        s2 = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=1,
            list_type=0, title="channel", owner=user,
        )
        s2_params = params.copy()
        s2_params["params"]["languages"] = ["ru"]
        CustomSegmentFileUpload.objects.create(segment=s2, query=s2_params)

        s3 = CustomSegment.objects.create(
            uuid=uuid.uuid4(), segment_type=1,
            list_type=0, title="channel", owner=user,
        )
        s3_params = params.copy()
        s3_params["params"]["languages"] = ["ga"]
        CustomSegmentFileUpload.objects.create(segment=s3, query=s3_params)

        query_params = QueryDict("general_data.top_lang_code=ga,ru").urlencode()
        response = self.client.get(f"{self._get_url('channel')}?{query_params}")
        self.assertEqual({s2.id, s3.id}, {int(item["id"]) for item in response.data["items"]})

    def test_owner_list_no_vetting(self):
        user = self.create_test_user()
        seg_1_params = dict(uuid=uuid.uuid4(), owner=user, list_type=0, segment_type=0, title="1")
        seg_2_params = dict(uuid=uuid.uuid4(), list_type=0, segment_type=0, title="2", audit_id=0)
        seg_1, _ = self._create_segment(segment_params=seg_1_params, export_params=dict(query={}))
        self._create_segment(segment_params=seg_2_params, export_params=dict(query={}))
        expected_segments_count = 1
        response = self.client.get(self._get_url("video"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_segments_count)
        self.assertEqual(response.data["items"][0]["id"], seg_1.id)

    def test_audit_vet_admin_list(self):
        """ Users with userprofile.vet_audit_admin permission should receive all segments """
        admin_user = self.create_test_user()
        admin_user.add_custom_user_group(PermissionGroupNames.AUDIT_VET_ADMIN)

        test_user_1 = self._create_user()
        test_user_2 = self._create_user()
        test_user_3 = self._create_user()
        seg_1, _ = self._create_segment(dict(owner=test_user_1, segment_type=1, title="test1", list_type=0, audit_id=1))
        seg_2, _ = self._create_segment(dict(owner=test_user_2, segment_type=1, title="test2", list_type=0))
        seg_3, _ = self._create_segment(dict(owner=test_user_3, segment_type=1, title="test3", list_type=0))

        response = self.client.get(self._get_url("channel"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 3)
        self.assertEqual({item["id"] for item in response.data["items"]}, {seg_1.id, seg_2.id, seg_3.id})

    def test_audit_vetter_list(self):
        """ Users with userprofile.vet_audit permission should receive only lists with vetting enabled """
        vetting_user = self.create_test_user()
        vetting_user.add_custom_user_group(PermissionGroupNames.AUDIT_VET)

        test_user_1 = self._create_user()
        test_user_2 = self._create_user()
        test_user_3 = self._create_user()
        seg_1, _ = self._create_segment(dict(owner=test_user_1, segment_type=0, title="test1", list_type=0, audit_id=1))
        self._create_segment(dict(owner=test_user_2, segment_type=0, title="test2", list_type=0))
        seg_3, _ = self._create_segment(dict(owner=test_user_3, segment_type=0, title="test3", list_type=0, audit_id=2))

        response = self.client.get(self._get_url("video"))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        self.assertEqual({item["id"] for item in response.data["items"]}, {seg_1.id, seg_3.id})

    def test_vetting_complete(self):
        vetting_user = self.create_test_user()
        vetting_user.add_custom_user_group(PermissionGroupNames.AUDIT_VET_ADMIN)

        test_user_1 = self._create_user()
        test_user_2 = self._create_user()
        test_user_3 = self._create_user()
        seg_1, _ = self._create_segment(dict(owner=test_user_1, segment_type=0, title="test1", list_type=0,
                                             audit_id=next(int_iterator), is_vetting_complete=True))
        seg_2, _ = self._create_segment(dict(owner=test_user_2, segment_type=0, title="test2", list_type=0,
                                             audit_id=next(int_iterator)))
        self._create_segment(dict(owner=test_user_3, segment_type=0, title="test3", list_type=0))

        AuditProcessor.objects.create(id=seg_1.audit_id)
        AuditProcessor.objects.create(id=seg_2.audit_id)

        response = self.client.get(self._get_url("video"))
        data = response.data
        data["items"].sort(key=lambda x: x["id"])
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(data["items_count"], 3)
        self.assertEqual(data["items"][0].get("is_vetting_complete"), True)
        self.assertEqual(data["items"][1].get("is_vetting_complete"), False)
        self.assertEqual(data["items"][2].get("is_vetting_complete"), False)

    def test_create_perm_can_download(self):
        """ Test users with create ctl permission can view and download their own list """
        user_1 = self.create_test_user()
        user_2 = self._create_user()
        user_2.add_custom_user_group(PermissionGroupNames.CUSTOM_TARGET_LIST_CREATION)
        _, export = self._create_segment(dict(owner=user_1, segment_type=0, title="test_1", list_type=0),
                                         export_params=dict(
                                             query={},
                                             download_url="test_1_url"
                                         ))
        self._create_segment(dict(owner=user_2, segment_type=0, title="test_2", list_type=0),
                             export_params=dict(query={}, download_url="test_2_url"))
        # request uses last user created
        response = self.client.get(self._get_url("video"))
        data = response.data
        owned = data["items"][0]
        self.assertEqual(owned["owner_id"], str(user_1.id))
        self.assertEqual(owned["download_url"], export.download_url)
