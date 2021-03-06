from datetime import timedelta
from uuid import uuid4
import json

from django.utils import timezone
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import RequestError
from mock import patch
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from .utils import create_test_audit_objects
from audit_tool.api.serializers.audit_channel_vet_serializer import AuditChannelVetSerializer
from audit_tool.api.serializers.audit_video_vet_serializer import AuditVideoVetSerializer
from audit_tool.api.urls.names import AuditPathName
from audit_tool.api.views.audit_vet_retrieve_update import CHECKOUT_THRESHOLD
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoVet
from audit_tool.models import get_hash_name
from brand_safety.models import BadWordCategory
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from saas.urls.namespaces import Namespace
from segment.models.constants import SegmentTypeEnum
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


@patch("audit_tool.api.views.audit_vet_retrieve_update.generate_vetted_segment")
class AuditVetRetrieveUpdateTestCase(ExtendedAPITestCase):
    custom_segment_model = None
    custom_segment_export_model = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        create_test_audit_objects()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def setUp(self):
        # Import and set models to avoid recursive ImportError
        from segment.models import CustomSegment
        from segment.models import CustomSegmentFileUpload
        self.custom_segment_model = CustomSegment
        self.custom_segment_export_model = CustomSegmentFileUpload
        self.patcher = patch(
            "audit_tool.api.views.audit_vet_retrieve_update.AuditVetRetrieveUpdateAPIView._get_document")
        self.mock_get_document = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def _get_url(self, kwargs):
        url = reverse(AuditPathName.AUDIT_VET, [Namespace.AUDIT_TOOL], kwargs=kwargs)
        return url

    def _create_segment_audit(self, user, audit_params=None, segment_params=None):
        default_audit_params = dict(source=1, audit_type=2, params=dict(instructions="test instructions"),
                                    completed=timezone.now())
        default_segment_params = dict(
            owner=user, title="test", segment_type=0, list_type=0, statistics={"items_count": 1}, uuid=uuid4()
        )
        default_audit_params.update(audit_params or {})
        default_segment_params.update(segment_params or {})
        audit = AuditProcessor.objects.create(**default_audit_params)
        segment = self.custom_segment_model.objects.create(audit_id=audit.id, **default_segment_params)
        self.custom_segment_export_model.objects.create(segment=segment, query={})
        return audit, segment

    def _create_mock_document(self, model, item_id, task_us_data=None, monetzation_data=None, general_data=None):
        default_task_us = {}
        default_monetzation = {}
        default_task_us.update(task_us_data or {})
        default_monetzation.update(monetzation_data or {})
        general_data = general_data or {}
        doc = model(item_id)
        doc.populate_task_us_data(**default_task_us)
        doc.populate_monetization(**default_monetzation)
        doc.populate_general_data(**general_data)
        return doc

    def test_reject_permissions(self, *args):
        self.create_test_user()
        url = self._get_url(kwargs=dict(pk=1))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(self.mock_get_document.call_count, 0)

    def test_get_next_video_vetting_item_with_history_success(self, *args):
        """ Test retrieving next vetting item in video audit with history """
        user = self.create_admin_user()
        before = timezone.now()
        audit_1, segment_1 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=0, title="test_title_1"))
        audit_2, segment_2 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=0, title="test_title_2"))
        audit_3, segment_3 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=0, title="test_title_3"))
        v_id = f"video{next(int_iterator)}"
        video_audit = AuditVideo.objects.create(video_id=v_id, video_id_hash=get_hash_name(v_id))
        video_meta = AuditVideoMeta.objects.create(video=video_audit, name="test meta name")
        historical_video_vet_1 = AuditVideoVet.objects.create(
            audit=audit_1, video=video_audit, processed=before, clean=False, processed_by_user_id=user.id
        )
        historical_video_vet_2 = AuditVideoVet.objects.create(
            audit=audit_2, video=video_audit, processed=before, clean=True, processed_by_user_id=user.id
        )
        new_video_vet = AuditVideoVet.objects.create(audit=audit_3, video=video_audit)

        self.assertEqual(new_video_vet.processed, None)
        self.assertEqual(new_video_vet.checked_out_at, None)
        task_us = dict(
            lang_code="en",
            age_group="1",
            gender="2",
            brand_safety=["3", "4"],
            iab_categories=["Action Video Games"],
            content_type="1",
            content_quality="0",
        )
        monetization = dict(is_monetizable=True)
        general_data = dict(primary_category="Video Games")
        mock_video_doc = self._create_mock_document(Video, video_audit.video_id, task_us_data=task_us,
                                                    monetzation_data=monetization, general_data=general_data)
        self.mock_get_document.return_value = mock_video_doc
        url = self._get_url(kwargs=dict(pk=audit_3.id))
        response = self.client.get(url)
        data = response.data
        serialized = AuditVideoVetSerializer(mock_video_doc)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(data["language"], serialized.data["language"])
        self.assertEqual(data["age_group"], serialized.data["age_group"])
        self.assertEqual(data["gender"], serialized.data["gender"])
        self.assertEqual(data["brand_safety"], serialized.data["brand_safety"])
        self.assertEqual(data["iab_categories"], serialized.data["iab_categories"])
        self.assertEqual(data["content_type"], serialized.data["content_type"])
        self.assertEqual(data["content_quality"], serialized.data["content_quality"])
        self.assertEqual(data["is_monetizable"], monetization["is_monetizable"])
        self.assertEqual(data["primary_category"], general_data["primary_category"])
        self.assertEqual(data["segment_title"], segment_3.title)
        self.assertEqual(data["url"], f"https://www.youtube.com/watch?v={video_audit.video_id}/")
        self.assertEqual(data["vetting_id"], new_video_vet.id)
        self.assertEqual(data["suitable"], new_video_vet.clean)
        self.assertTrue(before < data["checked_out_at"])

        vetting_history = sorted(data["vetting_history"], key=lambda item: item["suitable"])
        self.assertEqual(vetting_history[0]["suitable"], historical_video_vet_1.clean)
        self.assertEqual(vetting_history[0]["processed_by"], str(user))
        self.assertTrue(video_meta.name in vetting_history[0]["data"])
        self.assertEqual(vetting_history[1]["suitable"], historical_video_vet_2.clean)
        self.assertEqual(vetting_history[0]["processed_by"], str(user))
        self.assertTrue(video_meta.name in vetting_history[1]["data"])

    def test_get_next_channel_vetting_item_with_history_success(self, *args):
        """ Test retrieving next vetting item in channel audit with history """
        user = self.create_admin_user()
        before = timezone.now()
        audit_1, segment_1 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=1, title="test_title_1"))
        audit_2, segment_2 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=1, title="test_title_2"))
        audit_3, segment_3 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=1, title="test_title_3"))
        c_id = f"test_youtube_channel_id{next(int_iterator)}"
        channel_audit = AuditChannel.objects.create(channel_id=c_id, channel_id_hash=get_hash_name(c_id))
        channel_meta = AuditChannelMeta.objects.create(channel=channel_audit, name="test meta name")
        historical_channel_vet_1 = AuditChannelVet.objects.create(
            audit=audit_1, channel=channel_audit, processed=before, clean=False, processed_by_user_id=user.id
        )
        historical_video_vet_2 = AuditChannelVet.objects.create(
            audit=audit_2, channel=channel_audit, processed=before, clean=True, processed_by_user_id=user.id
        )
        new_channel_vet = AuditChannelVet.objects.create(audit=audit_3, channel=channel_audit, clean=True)

        self.assertEqual(new_channel_vet.processed, None)
        self.assertEqual(new_channel_vet.checked_out_at, None)
        iab_categories = ['Video Gaming', 'PC Games', 'MMOs', 'asdf']
        invalid_iab_categories = ['asdf']
        valid_iab_categories = list(set(iab_categories) - set(invalid_iab_categories))
        task_us = dict(
            lang_code="ko",
            age_group="0",
            gender="2",
            brand_safety=["1", "2"],
            iab_categories=iab_categories,
            content_type="2",
            content_quality="2",
        )
        monetization = dict(is_monetizable=False)
        general_data = dict(primary_category="Video Games")
        mock_channel_doc = self._create_mock_document(Channel, channel_audit.channel_id, task_us_data=task_us,
                                                      monetzation_data=monetization, general_data=general_data)
        self.mock_get_document.return_value = mock_channel_doc
        url = self._get_url(kwargs=dict(pk=audit_3.id))
        response = self.client.get(url)
        data = response.data
        serialized = AuditChannelVetSerializer(mock_channel_doc)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(data["language"], serialized.data["language"])
        self.assertEqual(data["age_group"], serialized.data["age_group"])
        self.assertEqual(data["gender"], serialized.data["gender"])
        self.assertEqual(data["brand_safety"], serialized.data["brand_safety"])
        self.assertCountEqual(data["iab_categories"], valid_iab_categories)
        self.assertEqual(data["content_type"], serialized.data["content_type"])
        self.assertEqual(data["content_quality"], serialized.data["content_quality"])
        self.assertEqual(data["is_monetizable"], monetization["is_monetizable"])
        self.assertEqual(data["primary_category"], general_data["primary_category"])
        self.assertEqual(data["segment_title"], segment_3.title)
        self.assertEqual(data["url"], f"https://www.youtube.com/channel/{channel_audit.channel_id}/")
        self.assertEqual(data["vetting_id"], new_channel_vet.id)
        self.assertEqual(data["suitable"], new_channel_vet.clean)
        self.assertTrue(before < data["checked_out_at"])

        vetting_history = sorted(data["vetting_history"], key=lambda item: item["suitable"])
        self.assertEqual(vetting_history[0]["suitable"], historical_channel_vet_1.clean)
        self.assertEqual(vetting_history[0]["processed_by"], str(user))
        self.assertTrue(channel_meta.name in vetting_history[0]["data"])
        self.assertEqual(vetting_history[1]["suitable"], historical_video_vet_2.clean)
        self.assertEqual(vetting_history[1]["processed_by"], str(user))
        self.assertTrue(channel_meta.name in vetting_history[1]["data"])

    def test_get_next_video_vetting_item_missing(self, *args):
        """ Test handling retrieving next vetting item with missing AuditVideo or invalid video_id """
        user = self.create_admin_user()
        audit_1, segment_1 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=0, title="test_title_1"))
        audit_2, segment_2 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=0, title="test_title_2"))
        audit_3, segment_3 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=0, title="test_title_2"))

        v_id_1 = f"video{next(int_iterator)}"
        v_id_2 = ""

        video_audit_1 = AuditVideo.objects.create(video_id=v_id_1, video_id_hash=get_hash_name(v_id_1))
        AuditVideoMeta.objects.create(video=video_audit_1, name="test meta name")

        video_audit_2 = AuditVideo.objects.create(video_id=v_id_2, video_id_hash=get_hash_name(v_id_2))
        AuditVideoMeta.objects.create(video=video_audit_2, name="test meta name")

        AuditVideoVet.objects.create(audit=audit_1, video=video_audit_1)
        AuditVideoVet.objects.create(audit=audit_2, video=video_audit_2)
        AuditVideoVet.objects.create(audit=audit_3)

        self.patcher.stop()
        with patch.object(VideoManager, "get") as mock_get:
            mock_get.side_effect = NotFoundError
            url = self._get_url(kwargs=dict(pk=audit_1.id))
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data["message"])

        with patch.object(VideoManager, "get") as mock_get:
            mock_get.side_effect = RequestError
            url = self._get_url(kwargs=dict(pk=audit_2.id))
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data["message"])

        # Handle missing VideoAudit
        url = self._get_url(kwargs=dict(pk=audit_3.id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data["message"])

    def test_get_next_channel_vetting_item_missing(self, *args):
        """ Test handling retrieving next vetting item with missing AuditChannel or invalid channel_id """
        user = self.create_admin_user()
        timezone.now()
        audit_1, segment_1 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=1, title="test_title_1"))
        audit_2, segment_2 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=1, title="test_title_2"))
        audit_3, segment_3 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=1, title="test_title_2"))

        c_id_1 = f"channel_test_id_{next(int_iterator)}"
        c_id_2 = ""
        channel_audit_1 = AuditChannel.objects.create(channel_id=c_id_1, channel_id_hash=get_hash_name(c_id_1))
        AuditChannelMeta.objects.create(channel=channel_audit_1, name="test meta name")

        channel_audit_2 = AuditChannel.objects.create(channel_id=c_id_2, channel_id_hash=get_hash_name(c_id_2))
        AuditChannelMeta.objects.create(channel=channel_audit_2, name="test meta name")

        AuditChannelVet.objects.create(audit=audit_1, channel=channel_audit_1)
        AuditChannelVet.objects.create(audit=audit_2, channel=channel_audit_2)
        AuditChannelVet.objects.create(audit=audit_3)

        self.patcher.stop()
        with patch.object(ChannelManager, "get") as mock_get:
            mock_get.side_effect = NotFoundError
            url = self._get_url(kwargs=dict(pk=audit_1.id))
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data["message"])

        with patch.object(ChannelManager, "get") as mock_get:
            mock_get.side_effect = RequestError
            url = self._get_url(kwargs=dict(pk=audit_2.id))
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data["message"])

        # Handle missing ChannelAudit
        url = self._get_url(kwargs=dict(pk=audit_3.id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data["message"])

    def test_patch_required_parameters(self, *args):
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_1"))
        audit_item_yt_id = f"video{next(int_iterator)}"
        audit_item = AuditVideo.objects.create(video_id=audit_item_yt_id)
        AuditVideoMeta.objects.create(video=audit_item, name="test meta name")
        vetting_item = AuditVideoVet.objects.create(audit=audit, video=audit_item, checked_out_at=before)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 0,
            "is_monetizable": True,
            "language": "es",
            "suitable": False
        }
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_patch_video_vetting_item_success(self, mock_generate_vetted):
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_1"))
        audit_item_yt_id = f"video{next(int_iterator)}"
        audit_items = [
            AuditVideo.objects.create(video_id=audit_item_yt_id),
            AuditVideo.objects.create(video_id=f"video{next(int_iterator)}")
        ]
        AuditVideoMeta.objects.create(video=audit_items[0], name="test meta name")
        AuditVideoMeta.objects.create(video=audit_items[1], name="test meta name")
        vetting_items = [
            AuditVideoVet.objects.create(audit=audit, video=audit_items[0], checked_out_at=before),
            AuditVideoVet.objects.create(audit=audit, video=audit_items[1])
        ]
        vetting_item = vetting_items[0]
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 0,
            "brand_safety": [
                12
            ],
            "content_type": 2,
            "content_quality": 0,
            "gender": 0,
            "iab_categories": [
                "Motorcycles"
            ],
            "primary_category": "Automotive",
            "is_monetizable": True,
            "language": "es",
            "suitable": False
        }
        BadWordCategory.objects.create(id=12, name="test_category")
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
            "audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer._update_channel"), \
             patch("audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer.save_elasticsearch"):
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        vetting_item.refresh_from_db()
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertEqual(data["task_us_data"]["age_group"], str(payload["age_group"]))
        self.assertEqual(data["task_us_data"]["brand_safety"], [_id for _id in payload["brand_safety"]])
        self.assertEqual(data["task_us_data"]["content_type"], str(payload["content_type"]))
        self.assertEqual(data["task_us_data"]["content_quality"], str(payload["content_quality"]))
        self.assertEqual(data["task_us_data"]["gender"], str(payload["gender"]))
        self.assertEqual(data["task_us_data"]["language"], payload["language"])
        self.assertEqual(data["task_us_data"]["iab_categories"], [str(_id) for _id in payload["iab_categories"]])
        self.assertEqual(payload["suitable"], vetting_item.clean)
        self.assertEqual(data["monetization"]["is_monetizable"], payload["is_monetizable"])
        self.assertTrue(vetting_item.processed > before)
        self.assertIsNone(vetting_item.checked_out_at)
        self.assertEqual(mock_generate_vetted.delay.call_count, 0)

    def test_patch_channel_vetting_item_success(self, mock_generate_vetted):
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_1"))
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        audit_items = [
            AuditChannel.objects.create(channel_id=audit_item_yt_id),
            AuditChannel.objects.create(channel_id=f"test_youtube_channel_id{next(int_iterator)}")
        ]
        AuditChannelMeta.objects.create(channel=audit_items[0], name="test meta name")
        AuditChannelMeta.objects.create(channel=audit_items[1], name="test meta name")

        vetting_items = [
            AuditChannelVet.objects.create(audit=audit, channel=audit_items[0], checked_out_at=before),
            AuditChannelVet.objects.create(audit=audit, channel=audit_items[1])
        ]

        vetting_item = vetting_items[0]
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 2,
            "brand_safety": [
                11, 4
            ],
            "content_type": 1,
            "content_quality": 2,
            "gender": 2,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries"
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.create(id=4, name="test_category_4")
        BadWordCategory.objects.create(id=11, name="test_category_11")
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
            "audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.save_elasticsearch") as mock_save_es:
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        vetting_item.refresh_from_db()
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertEqual(data["task_us_data"]["age_group"], str(payload["age_group"]))
        self.assertEqual(data["task_us_data"]["brand_safety"], [_id for _id in payload["brand_safety"]])
        self.assertEqual(data["task_us_data"]["content_type"], str(payload["content_type"]))
        self.assertEqual(data["task_us_data"]["content_quality"], str(payload["content_quality"]))
        self.assertEqual(data["task_us_data"]["gender"], str(payload["gender"]))
        self.assertEqual(data["task_us_data"]["language"], payload["language"])
        self.assertEqual(sorted(data["task_us_data"]["iab_categories"]),
                         sorted([_id for _id in payload["iab_categories"]]))
        self.assertEqual(payload["suitable"], vetting_item.clean)
        self.assertEqual(data["monetization"]["is_monetizable"], payload["is_monetizable"])
        self.assertTrue(vetting_item.processed > before)
        self.assertIsNone(vetting_item.checked_out_at)
        self.assertEqual(mock_generate_vetted.delay.call_count, 0)

    def test_handle_video_skip_not_exists(self, *args):
        """
        Test handling skipping video vet
        If skipped_type is 0, then skipped since item is unavailable e.g. deleted from Youtube
            Should set clean to False, skipped to False
        If skipped_type is 1, then skipped since unavailable in region
            Should set clean to False, skipped to True
        """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_1"))
        audit_item_yt_id = f"video{next(int_iterator)}"
        audit_item = AuditVideo.objects.create(video_id=audit_item_yt_id)
        AuditVideoMeta.objects.create(video=audit_item, name="test meta name")
        vetting_item = AuditVideoVet.objects.create(audit=audit, video=audit_item, checked_out_at=before)
        payload = {
            "vetting_id": vetting_item.id,
            "skipped": 0
        }
        response = self.client.patch(self._get_url(kwargs=dict(pk=audit.id)), data=json.dumps(payload),
                                     content_type="application/json")
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.clean, False)
        self.assertEqual(vetting_item.skipped, False)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertTrue(vetting_item.processed > before)

    def test_handle_video_skip_not_available(self, *args):
        """
        Test handling skipping video vet
        If skipped_type is 0, then skipped since item is unavailable e.g. deleted from Youtube
            Should set clean to False, skipped to False
        If skipped_type is 1, then skipped since unavailable in region
            Should set clean to False, skipped to True
        """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_1"))
        audit_item_yt_id = f"video{next(int_iterator)}"
        audit_item = AuditVideo.objects.create(video_id=audit_item_yt_id)
        AuditVideoMeta.objects.create(video=audit_item, name="test meta name")
        vetting_item = AuditVideoVet.objects.create(audit=audit, video=audit_item, checked_out_at=before)
        payload = {
            "vetting_id": vetting_item.id,
            "skipped": 1
        }
        response = self.client.patch(self._get_url(kwargs=dict(pk=audit.id)), data=json.dumps(payload),
                                     content_type="application/json")
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.clean, False)
        self.assertEqual(vetting_item.skipped, True)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertTrue(vetting_item.processed > before)

    def test_handle_channel_skip_not_exists(self, *args):
        """
        Test handling skipping channel vet
        If skipped_type is 0, then skipped since item is unavailable e.g. deleted from Youtube
            Should set clean to False, skipped to False
        If skipped_type is 1, then skipped since unavailable in region
            Should set clean to False, skipped to True
        """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_1"))
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        audit_item = AuditChannel.objects.create(channel_id=audit_item_yt_id)
        AuditChannelMeta.objects.create(channel=audit_item, name="test meta name")
        vetting_item = AuditChannelVet.objects.create(audit=audit, channel=audit_item, checked_out_at=before)
        payload = {
            "vetting_id": vetting_item.id,
            "skipped": 0
        }
        response = self.client.patch(self._get_url(kwargs=dict(pk=audit.id)), data=json.dumps(payload),
                                     content_type="application/json")
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.clean, False)
        self.assertEqual(vetting_item.skipped, False)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertTrue(vetting_item.processed > before)

    def test_handle_channel_skip_not_available(self, *args):
        """
        Test handling skipping channel vet
        If skipped_type is 0, then skipped since item is unavailable e.g. deleted from Youtube
            Should set clean to False, skipped to False
        If skipped_type is 1, then skipped since unavailable in region
            Should set clean to False, skipped to True
        """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_1"))
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        audit_item = AuditChannel.objects.create(channel_id=audit_item_yt_id)
        AuditChannelMeta.objects.create(channel=audit_item, name="test meta name")
        vetting_item = AuditChannelVet.objects.create(audit=audit, channel=audit_item, checked_out_at=before)
        payload = {
            "vetting_id": vetting_item.id,
            "skipped": 1
        }
        response = self.client.patch(self._get_url(kwargs=dict(pk=audit.id)), data=json.dumps(payload),
                                     content_type="application/json")
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.clean, False)
        self.assertEqual(vetting_item.skipped, True)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertTrue(vetting_item.processed > before)

    def test_get_video_all_checked_out(self, *args):
        """ Test handling all vetting items are checked out """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_1"))
        audit_item_yt_id = f"video{next(int_iterator)}"
        audit_item = AuditVideo.objects.create(video_id=audit_item_yt_id,
                                               video_id_hash=get_hash_name(audit_item_yt_id))
        AuditVideoMeta.objects.create(video=audit_item, name="test meta name")
        vetting_items = [AuditVideoVet(audit=audit, video=audit_item, checked_out_at=before, processed=before)]
        AuditVideoVet.objects.bulk_create(vetting_items)
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["message"], "All items are checked out. Please request from a different list.")
        self.assertEqual(self.mock_get_document.call_count, 0)

    def test_get_channel_all_checked_out(self, *args):
        """ Test handling all vetting items are checked out """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_1"))
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        audit_item = AuditChannel.objects.create(channel_id=audit_item_yt_id,
                                                 channel_id_hash=get_hash_name(audit_item_yt_id))
        AuditChannelMeta.objects.create(channel=audit_item, name="test meta name")
        vetting_items = [AuditChannelVet(audit=audit, channel=audit_item, checked_out_at=before, processed=before)]
        AuditChannelVet.objects.bulk_create(vetting_items)
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["message"], "All items are checked out. Please request from a different list.")
        self.assertEqual(self.mock_get_document.call_count, 0)

    def test_handle_get_video_vetting_completed(self, *args):
        """ Handle getting next item for completed lists. Should return message notifying user list is completed """
        user = self.create_admin_user()
        timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title",
                                                                              is_vetting_complete=True))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
            "audit_tool.api.views.audit_vet_retrieve_update.AuditVetRetrieveUpdateAPIView._retrieve_next_vetting_item") as mock_retrieve:
            mock_retrieve.return_value = None
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["message"],
                         "Vetting for this list is complete. Please move on to the next list.")
        self.assertEqual(self.mock_get_document.call_count, 0)
        self.assertEqual(mock_retrieve.call_count, 0)

    def test_handle_get_channel_vetting_completed(self, *args):
        """ Handle getting next item for completed lists. Should return message notifying user list is completed """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title",
                                                                              is_vetting_complete=True))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
            "audit_tool.api.views.audit_vet_retrieve_update.AuditVetRetrieveUpdateAPIView._retrieve_next_vetting_item") as mock_retrieve:
            mock_retrieve.return_value = None
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["message"],
                         "Vetting for this list is complete. Please move on to the next list.")
        self.assertEqual(self.mock_get_document.call_count, 0)
        self.assertEqual(mock_retrieve.call_count, 0)

    def test_patch_video_last_vetting_item_success(self, mock_generate_vetted):
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_1"))
        audit_item_yt_id = f"video{next(int_iterator)}"
        audit_item = AuditVideo.objects.create(video_id=audit_item_yt_id)
        AuditVideoMeta.objects.create(video=audit_item, name="test meta name")
        vetting_item = AuditVideoVet.objects.create(audit=audit, video=audit_item, checked_out_at=before)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 0,
            "brand_safety": [
                12
            ],
            "content_type": 2,
            "content_quality": 1,
            "gender": 0,
            "iab_categories": [
                "Motorcycles"
            ],
            "primary_category": "Automotive",
            "is_monetizable": True,
            "language": "es",
            "suitable": False
        }
        BadWordCategory.objects.create(id=12, name="test_category")
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
            "audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer._update_channel") as mock_update_channel, \
            patch(
                "audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer.save_elasticsearch") as mock_save_es:
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        mock_generate_vetted.delay.assert_called_once()

    def test_patch_channel_last_vetting_item_success(self, mock_generate_vetted):
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_1"))
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        audit_item = AuditChannel.objects.create(channel_id=audit_item_yt_id)
        AuditChannelMeta.objects.create(channel=audit_item, name="test meta name")
        vetting_item = AuditChannelVet.objects.create(audit=audit, channel=audit_item, checked_out_at=before)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 2,
            "brand_safety": [
                4, 11
            ],
            "content_type": 1,
            "content_quality": 0,
            "gender": 2,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries"
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.create(id=4, name="test_category_4")
        BadWordCategory.objects.create(id=11, name="test_category_11")
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
            "audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.save_elasticsearch") as mock_save_es:
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        mock_generate_vetted.delay.assert_called_once()

    def test_channel_history_no_meta_success(self, *args):
        """ Test retrieving history for channels with no AuditChannelMeta does not throw exception """
        user = self.create_admin_user()
        before = timezone.now()
        audit_1, segment_1 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=1, title="test_title_1"))
        audit_2, segment_2 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=1, title="test_title_2"))
        audit_3, segment_3 = self._create_segment_audit(user,
                                                        segment_params=dict(segment_type=1, title="test_title_3"))
        c_id = f"test_youtube_channel_id{next(int_iterator)}"
        # AuditChannelMeta is usually created with AuditChannel
        channel_audit = AuditChannel.objects.create(channel_id=c_id, channel_id_hash=get_hash_name(c_id))
        AuditChannelVet.objects.create(
            audit=audit_1, channel=channel_audit, processed=before, clean=False
        )
        AuditChannelVet.objects.create(
            audit=audit_2, channel=channel_audit, processed=before, clean=True
        )
        new_channel_vet = AuditChannelVet.objects.create(audit=audit_3, channel=channel_audit, clean=True)

        self.assertEqual(new_channel_vet.processed, None)
        self.assertEqual(new_channel_vet.checked_out_at, None)
        iab_categories = ['Video Gaming', 'PC Games', 'MMOs']
        task_us = dict(iab_categories=iab_categories)
        monetization = dict(is_monetizable=False)
        general_data = dict(primary_category="Video Games")
        mock_channel_doc = self._create_mock_document(Channel, channel_audit.channel_id, task_us_data=task_us,
                                                      monetzation_data=monetization, general_data=general_data)
        self.mock_get_document.return_value = mock_channel_doc
        url = self._get_url(kwargs=dict(pk=audit_3.id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["vetting_id"], new_channel_vet.id)

    def test_patch_channel_primary_updates_iab_categories(self, *args):
        """ Test that updating primary_category adds to general_data.iab_categories and task_us_data.iab_categories """
        user = self.create_admin_user()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_1"))
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        audit_item = AuditChannel.objects.create(channel_id=audit_item_yt_id)
        AuditChannelMeta.objects.create(channel=audit_item, name="test meta name")
        vetting_item = AuditChannelVet.objects.create(audit=audit, channel=audit_item)
        payload = {
            "brand_safety": [],
            "age_group": 1,
            "content_quality": 1,
            "content_type": 1,
            "gender": 1,
            "iab_categories": ["Industries"],
            "is_monetizable": False,
            "language": "ja",
            "primary_category": "Automotive",
            "suitable": True,
            "vetting_id": vetting_item.id,
        }
        mock_doc = self._create_mock_document(Channel, audit_item.channel_id)
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch("audit_tool.api.serializers.audit_vet_base_serializer.upsert_retry") as mock_upsert_retry,\
                patch.object(ChannelManager, "get", return_value=[mock_doc]):
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        upserted_doc = mock_upsert_retry.call_args_list[0].args[1][0]
        self.assertTrue(payload["primary_category"] in upserted_doc.general_data.iab_categories)
        self.assertTrue(payload["primary_category"] in upserted_doc.task_us_data.iab_categories)

    def test_patch_video_primary_updates_iab_categories(self, *args):
        """ Test that updating primary_category adds to general_data.iab_categories and task_us_data.iab_categories """
        user = self.create_admin_user()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="video"))
        audit_item_yt_id = f"video_id{next(int_iterator)}"
        audit_item = AuditVideo.objects.create(video_id=audit_item_yt_id)
        AuditVideoMeta.objects.create(video=audit_item, name="test meta name")
        vetting_item = AuditVideoVet.objects.create(audit=audit, video=audit_item)
        payload = {
            "brand_safety": [],
            "age_group": 1,
            "content_quality": 1,
            "content_type": 1,
            "gender": 1,
            "iab_categories": ["Industries"],
            "is_monetizable": False,
            "language": "ja",
            "primary_category": "Automotive",
            "suitable": True,
            "vetting_id": vetting_item.id,
        }
        mock_doc = self._create_mock_document(Video, audit_item.channel_id)
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch("audit_tool.api.serializers.audit_vet_base_serializer.upsert_retry") as mock_upsert_retry,\
                patch.object(VideoManager, "get", return_value=[mock_doc]):
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        upserted_doc = mock_upsert_retry.call_args_list[0].args[1][0]
        self.assertTrue(payload["primary_category"] in upserted_doc.general_data.iab_categories)
        self.assertTrue(payload["primary_category"] in upserted_doc.task_us_data.iab_categories)

    def test_check_in_threshold(self, *args):
        """
        Ensure that audit vet item check in/out is enforced
        :param args:
        :return:
        """
        user = self.create_admin_user()
        before = timezone.now() - timedelta(minutes=CHECKOUT_THRESHOLD + 5)
        after = timezone.now()
        video_audit, video_segment = self._create_segment_audit(user, segment_params=dict(
            segment_type=SegmentTypeEnum.VIDEO.value, title="test_title_1"))
        channel_audit, channel_segment = self._create_segment_audit(user, segment_params=dict(
            segment_type=SegmentTypeEnum.CHANNEL.value, title="test_title_2"))
        audit_videos = []
        audit_channels = []
        video_vets = []
        channel_vets = []
        range_max = 3
        for i in range(range_max):
            audit_video = AuditVideo.objects.create(video_id=f"video_id{next(int_iterator)}")
            audit_videos.append(audit_video)
            # Last indexed items will be checked out "after", meaning they have expired and will be able to be
            # checked out
            checked_out_at = before if i + 1 < range_max else after
            video_vets.append(AuditVideoVet(audit=video_audit, video=audit_video, checked_out_at=checked_out_at))
        for i in range(range_max):
            audit_channel = AuditChannel.objects.create(channel_id=f"channel_id{next(int_iterator)}")
            audit_channels.append(audit_channel)
            checked_out_at = before if i + 1 < range_max else after
            channel_vets.append(AuditChannelVet(audit=channel_audit, channel=audit_channel, checked_out_at=checked_out_at))

        AuditVideoVet.objects.bulk_create(video_vets)
        AuditChannelVet.objects.bulk_create(channel_vets)

        self.assertTrue(all(item.checked_out_at is not None for item in video_vets))
        self.assertTrue(all(item.checked_out_at is not None for item in channel_vets))

        for audit_video in audit_videos:
            with self.subTest(audit_video):
                mock_video_doc = self._create_mock_document(Video, audit_video.video_id)
                self.mock_get_document.return_value = mock_video_doc
                url = self._get_url(kwargs=dict(pk=video_audit.id))
                response = self.client.get(url)
                if audit_video.video_id == audit_videos[-1].video_id:
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    self.assertIn("message", response.data.keys())
                    self.assertIn("All items are checked out.", response.data.get("message", ""))
                    continue
                else:
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    self.assertIn("vetting_id", response.data.keys())
                    vetting_id = response.data.get("vetting_id")
                    valid_video_vet_ids = [video_vet.id for video_vet in video_vets[:-1]]
                    self.assertIn(vetting_id, valid_video_vet_ids)

        for audit_channel in audit_channels:
            with self.subTest(audit_channel):
                mock_channel_doc = self._create_mock_document(Video, audit_channel.channel_id)
                self.mock_get_document.return_value = mock_channel_doc
                url = self._get_url(kwargs=dict(pk=channel_audit.id))
                response = self.client.get(url)
                if audit_channel.channel_id == audit_channels[-1].channel_id:
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    self.assertIn("message", response.data.keys())
                    self.assertIn("All items are checked out.", response.data.get("message", ""))
                    continue
                else:
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    self.assertIn("vetting_id", response.data.keys())
                    vetting_id = response.data.get("vetting_id")
                    valid_channel_vet_ids = [channel_vet.id for channel_vet in channel_vets[:-1]]
                    self.assertIn(vetting_id, valid_channel_vet_ids)

    def test_vetting_checkout_random(self, *args):
        """ Test that checking out vetting items is randomized to reduce the chance two clients may be requesting
        the resource at the same time and resulting in checking out the same vetting item
        """
        user = self.create_admin_user()
        audit, segment = self._create_segment_audit(user, segment_params=dict(
            segment_type=SegmentTypeEnum.VIDEO.value, title="test_vetting_checkout_random"))

        self.mock_get_document.return_value = None
        vets = []
        for i in range(50):
            vets.append(AuditVideoVet(audit=audit))
        AuditVideoVet.objects.bulk_create(vets)
        vetting_ids = []
        for _ in range(len(vets)):
            url = self._get_url(kwargs=dict(pk=audit.id))
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            vetting_ids.append(response.data["vetting_id"])
        self.assertNotEqual(vetting_ids, list(sorted(vetting_ids)))

    def test_checkout_submit_success(self, *args):
        """ Test that all items are able to be checked out and all items are able to be submitted """
        user = self.create_admin_user()
        audit, segment = self._create_segment_audit(user, segment_params=dict(
            segment_type=SegmentTypeEnum.CHANNEL.value, title="test_checkout_submit_success"))

        self.mock_get_document.return_value = None
        vets = []
        for i in range(25):
            audit_obj = AuditChannel.objects.create(channel_id=f"channel{next(int_iterator)}".zfill(24))
            vets.append(AuditChannelVet(audit=audit, channel=audit_obj))
        AuditChannelVet.objects.bulk_create(vets)
        vetting_ids = []
        for _ in range(len(vets)):
            url = self._get_url(kwargs=dict(pk=audit.id))
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            vetting_ids.append(response.data["vetting_id"])

        for _id in vetting_ids:
            payload = {
                "vetting_id": _id,
                "age_group": 0,
                "brand_safety": [
                    12
                ],
                "content_type": 0,
                "content_quality": 0,
                "gender": 0,
                "iab_categories": [
                    "Automotive"
                ],
                "primary_category": "Automotive",
                "is_monetizable": True,
                "language": "en",
                "suitable": False
            }
            url = self._get_url(kwargs=dict(pk=audit.id))
            with patch.object(AuditChannelVetSerializer, "save_elasticsearch"),\
                    patch.object(AuditChannelVetSerializer, "_update_videos"):
                response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
                self.assertEqual(response.status_code, HTTP_200_OK)
                data = response.data
                self.assertIsNone(data["checked_out_at"])
                self.assertTrue(data["processed"])

        # Ensure all vetting items have been processed
        if AuditChannelVet.objects.filter(audit=audit, processed__isnull=True).count() == 0:
            segment.is_vetting_complete = True
            segment.save(update_fields=["is_vetting_complete"])
        # All items have been processed
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue("Vetting for this list is complete" in response.data["message"])
