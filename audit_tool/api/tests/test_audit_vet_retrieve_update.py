import json
from mock import patch

from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
from uuid import uuid4

from audit_tool.api.urls.names import AuditPathName
from audit_tool.models import AuditCategory
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoVet
from audit_tool.models import BlacklistItem
from audit_tool.models import get_hash_name
from brand_safety.models import BadWordCategory
from es_components.models import Channel
from es_components.models import Video
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.int_iterator import int_iterator


class AuditAdminTestCase(ExtendedAPITestCase):
    custom_segment_model = None
    custom_segment_export_model = None

    def setUp(self):
        # Import and set models to avoid recursive ImportError
        from segment.models import CustomSegment
        from segment.models import CustomSegmentFileUpload
        self.custom_segment_model = CustomSegment
        self.custom_segment_export_model = CustomSegmentFileUpload
        self.patcher = patch("audit_tool.api.views.audit_vet_retrieve_update.AuditVetRetrieveUpdateAPIView._get_document")
        self.mock_get_document = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def _get_url(self, kwargs):
        url = reverse(AuditPathName.AUDIT_VET, [Namespace.AUDIT_TOOL], kwargs=kwargs)
        return url

    def _create_segment_audit(self, user, audit_params=None, segment_params=None):
        default_audit_params = dict(source=1, audit_type=2, params=dict(instructions="test instructions"))
        default_segment_params = dict(
            owner=user, title="test", segment_type=0, list_type=0, statistics={"items_count": 1}, uuid=uuid4()
        )
        default_audit_params.update(audit_params or {})
        default_segment_params.update(segment_params or {})
        audit = AuditProcessor.objects.create(**default_audit_params)
        segment = self.custom_segment_model.objects.create(audit_id=audit.id, **default_segment_params)
        self.custom_segment_export_model.objects.create(segment=segment, query={})
        return audit, segment

    def _create_mock_document(self, model, item_id, task_us_data=None, monetzation_data=None):
        default_task_us = {}
        default_monetzation = {}
        default_task_us.update(task_us_data or {})
        default_monetzation.update(monetzation_data or {})
        doc = model(item_id)
        doc.populate_task_us_data(**default_task_us)
        doc.populate_monetization(**default_monetzation)
        return doc

    def test_get_next_video_vetting_item_with_history_success(self):
        """ Test retrieving next vetting item in video audit """
        user = self.create_admin_user()
        before = timezone.now()
        audit_1, segment_1 = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_1"))
        audit_2, segment_2 = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_2"))
        audit_3, segment_3 = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_3"))
        v_id = f"video{next(int_iterator)}"
        video_audit = AuditVideo.objects.create(video_id=v_id, video_id_hash=get_hash_name(v_id))
        video_meta = AuditVideoMeta.objects.create(video=video_audit, name="test meta name")
        historical_video_vet_1 = AuditVideoVet.objects.create(
            audit=audit_1, video=video_audit, processed=before, clean=False
        )
        historical_video_vet_2 = AuditVideoVet.objects.create(
            audit=audit_2, video=video_audit, processed=before, clean=True
        )
        new_video_vet = AuditVideoVet.objects.create(audit=audit_3, video=video_audit)

        self.assertEqual(new_video_vet.processed, None)
        self.assertEqual(new_video_vet.checked_out_at, None)
        task_us = dict(
            language="en",
            age_group="1",
            gender="2",
            brand_safety=["3", "4"],
            iab_categories=["Video Games"],
            content_type="1"
        )
        monetization = dict(is_monetizable=True)
        mock_video_doc = self._create_mock_document(Video, video_audit.video_id, task_us_data=task_us, monetzation_data=monetization)
        self.mock_get_document.return_value = mock_video_doc
        url = self._get_url(kwargs=dict(pk=audit_3.id))
        response = self.client.get(url)
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(data["language"], task_us["language"])
        self.assertEqual(data["age_group"], task_us["age_group"])
        self.assertEqual(data["gender"], task_us["gender"])
        self.assertEqual(data["brand_safety"], task_us["brand_safety"])
        self.assertEqual(data["iab_categories"], task_us["iab_categories"])
        self.assertEqual(data["content_type"], task_us["content_type"])
        self.assertEqual(data["is_monetizable"], monetization["is_monetizable"])
        self.assertEqual(data["segment_title"], segment_3.title)
        self.assertEqual(data["url"], f"https://www.youtube.com/watch?v={video_audit.video_id}/")
        self.assertEqual(data["vetting_id"], new_video_vet.id)
        self.assertTrue(before < data["checked_out_at"])

        vetting_history = sorted(data["vetting_history"], key=lambda item: item["suitable"])
        self.assertEqual(vetting_history[0]["suitable"], historical_video_vet_1.clean)
        self.assertTrue(video_meta.name in vetting_history[0]["data"])
        self.assertEqual(vetting_history[1]["suitable"], historical_video_vet_2.clean)
        self.assertTrue(video_meta.name in vetting_history[1]["data"])

    def test_get_next_channel_vetting_item_with_history_success(self):
        """ Test retrieving next vetting item in video audit """
        user = self.create_admin_user()
        before = timezone.now()
        audit_1, segment_1 = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_1"))
        audit_2, segment_2 = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_2"))
        audit_3, segment_3 = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_3"))
        c_id = f"test_youtube_channel_id{next(int_iterator)}"
        channel_audit = AuditChannel.objects.create(channel_id=c_id, channel_id_hash=get_hash_name(c_id))
        channel_meta = AuditChannelMeta.objects.create(channel=channel_audit, name="test meta name")
        historical_channel_vet_1 = AuditChannelVet.objects.create(
            audit=audit_1, channel=channel_audit, processed=before, clean=False
        )
        historical_video_vet_2 = AuditChannelVet.objects.create(
            audit=audit_2, channel=channel_audit, processed=before, clean=True
        )
        new_channel_vet = AuditChannelVet.objects.create(audit=audit_3, channel=channel_audit)

        self.assertEqual(new_channel_vet.processed, None)
        self.assertEqual(new_channel_vet.checked_out_at, None)
        task_us = dict(
            language="ko",
            age_group="0",
            gender="2",
            brand_safety=["1", "2"],
            iab_categories=["Hiking"],
            content_type="2"
        )
        monetization = dict(is_monetizable=False)
        mock_channel_doc = self._create_mock_document(Channel, channel_audit.channel_id, task_us_data=task_us, monetzation_data=monetization)
        self.mock_get_document.return_value = mock_channel_doc
        url = self._get_url(kwargs=dict(pk=audit_3.id))
        response = self.client.get(url)
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(data["language"], task_us["language"])
        self.assertEqual(data["age_group"], task_us["age_group"])
        self.assertEqual(data["gender"], task_us["gender"])
        self.assertEqual(data["brand_safety"], task_us["brand_safety"])
        self.assertEqual(data["iab_categories"], task_us["iab_categories"])
        self.assertEqual(data["content_type"], task_us["content_type"])
        self.assertEqual(data["is_monetizable"], monetization["is_monetizable"])
        self.assertEqual(data["segment_title"], segment_3.title)
        self.assertEqual(data["url"], f"https://www.youtube.com/channel/{channel_audit.channel_id}/")
        self.assertEqual(data["vetting_id"], new_channel_vet.id)
        self.assertTrue(before < data["checked_out_at"])

        vetting_history = sorted(data["vetting_history"], key=lambda item: item["suitable"])
        self.assertEqual(vetting_history[0]["suitable"], historical_channel_vet_1.clean)
        self.assertTrue(channel_meta.name in vetting_history[0]["data"])
        self.assertEqual(vetting_history[1]["suitable"], historical_video_vet_2.clean)
        self.assertTrue(channel_meta.name in vetting_history[1]["data"])

    def test_patch_video_vetting_item_success(self):
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
            "gender": 0,
            "iab_categories": [
                "Motorcycles"
            ],
            "is_monetizable": True,
            "language": "es",
            "suitable": False
        }
        BadWordCategory.objects.create(id=12, name="test_category")
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch("audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer._update_channel") as mock_update_channel,\
            patch("audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer.save_elasticsearch") as mock_save_es:
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        vetting_item.refresh_from_db()
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertEqual(data["task_us_data"]["age_group"], str(payload["age_group"]))
        self.assertEqual(data["task_us_data"]["brand_safety"], [str(_id) for _id in payload["brand_safety"]])
        self.assertEqual(data["task_us_data"]["content_type"], str(payload["content_type"]))
        self.assertEqual(data["task_us_data"]["gender"], str(payload["gender"]))
        self.assertEqual(data["task_us_data"]["language"], payload["language"])
        self.assertEqual(data["task_us_data"]["iab_categories"], [str(_id) for _id in payload["iab_categories"]])
        self.assertEqual(payload["suitable"], vetting_item.clean)
        self.assertEqual(data["monetization"]["is_monetizable"], payload["is_monetizable"])
        self.assertTrue(vetting_item.processed > before)
        self.assertIsNone(vetting_item.checked_out_at)

        blacklist_data = BlacklistItem.objects.get(item_id=audit_item_yt_id)
        self.assertEqual(set([str(_id) for _id in payload["brand_safety"]]), set(blacklist_data.blacklist_category.keys()))

    def test_patch_channel_vetting_item_success(self):
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
            "gender": 2,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries"
            ],
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.create(id=4, name="test_category_4")
        BadWordCategory.objects.create(id=11, name="test_category_11")
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch("audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.save_elasticsearch") as mock_save_es:
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        vetting_item.refresh_from_db()
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertEqual(data["task_us_data"]["age_group"], str(payload["age_group"]))
        self.assertEqual(data["task_us_data"]["brand_safety"], [str(_id) for _id in payload["brand_safety"]])
        self.assertEqual(data["task_us_data"]["content_type"], str(payload["content_type"]))
        self.assertEqual(data["task_us_data"]["gender"], str(payload["gender"]))
        self.assertEqual(data["task_us_data"]["language"], payload["language"])
        self.assertEqual(data["task_us_data"]["iab_categories"], [str(_id) for _id in payload["iab_categories"]])
        self.assertEqual(payload["suitable"], vetting_item.clean)
        self.assertEqual(data["monetization"]["is_monetizable"], payload["is_monetizable"])
        self.assertTrue(vetting_item.processed > before)
        self.assertIsNone(vetting_item.checked_out_at)

        blacklist_data = BlacklistItem.objects.get(item_id=audit_item_yt_id)
        self.assertEqual(set([str(_id) for _id in payload["brand_safety"]]), set(blacklist_data.blacklist_category.keys()))

    def test_handle_video_skip_not_exists(self):
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
        response = self.client.patch(self._get_url(kwargs=dict(pk=audit.id)), data=json.dumps(payload), content_type="application/json")
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.clean, False)
        self.assertEqual(vetting_item.skipped, False)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertTrue(vetting_item.processed > before)

    def test_handle_video_skip_not_available(self):
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
        response = self.client.patch(self._get_url(kwargs=dict(pk=audit.id)), data=json.dumps(payload), content_type="application/json")
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.clean, False)
        self.assertEqual(vetting_item.skipped, True)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertTrue(vetting_item.processed > before)

    def test_handle_channel_skip_not_exists(self):
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
        response = self.client.patch(self._get_url(kwargs=dict(pk=audit.id)), data=json.dumps(payload), content_type="application/json")
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.clean, False)
        self.assertEqual(vetting_item.skipped, False)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertTrue(vetting_item.processed > before)

    def test_handle_channel_skip_not_available(self):
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
        response = self.client.patch(self._get_url(kwargs=dict(pk=audit.id)), data=json.dumps(payload), content_type="application/json")
        print(response.data)
        vetting_item.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(vetting_item.clean, False)
        self.assertEqual(vetting_item.skipped, True)
        self.assertEqual(vetting_item.processed_by_user_id, user.id)
        self.assertTrue(vetting_item.processed > before)

    def test_get_video_all_checked_out(self):
        """ Test handling all vetting items are checked out """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=0, title="test_title_1"))
        audit_item_yt_id = f"video{next(int_iterator)}"
        audit_item = AuditVideo.objects.create(video_id=audit_item_yt_id, video_id_hash=get_hash_name(audit_item_yt_id))
        audit_meta = AuditVideoMeta.objects.create(video=audit_item, name="test meta name")
        vetting_items = [AuditVideoVet(audit=audit, video=audit_item, checked_out_at=before, processed=before)]
        AuditVideoVet.objects.bulk_create(vetting_items)
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(self.mock_get_document.call_count, 0)

    def test_get_channel_all_checked_out(self):
        """ Test handling all vetting items are checked out """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_1"))
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        audit_item = AuditChannel.objects.create(channel_id=audit_item_yt_id, channel_id_hash=get_hash_name(audit_item_yt_id))
        audit_meta = AuditChannelMeta.objects.create(channel=audit_item, name="test meta name")
        vetting_items = [AuditChannelVet(audit=audit, channel=audit_item, checked_out_at=before, processed=before)]
        AuditChannelVet.objects.bulk_create(vetting_items)
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(self.mock_get_document.call_count, 0)

    def test_reject_video_vetting_completed(self):
        """ Reject getting next item for completed lists """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, audit_params=dict(completed=before), segment_params=dict(segment_type=0, title="test_title"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch("audit_tool.api.views.audit_vet_retrieve_update.AuditVetRetrieveUpdateAPIView._retrieve_next_vetting_item") as mock_retrieve:
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(self.mock_get_document.call_count, 0)
        self.assertEqual(mock_retrieve.call_count, 0)

    def test_reject_channel_vetting_completed(self):
        """ Reject getting next item for completed lists """
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, audit_params=dict(completed=before), segment_params=dict(segment_type=1, title="test_title"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch("audit_tool.api.views.audit_vet_retrieve_update.AuditVetRetrieveUpdateAPIView._retrieve_next_vetting_item") as mock_retrieve:
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(self.mock_get_document.call_count, 0)
        self.assertEqual(mock_retrieve.call_count, 0)
