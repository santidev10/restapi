import json
from uuid import uuid4

from mock import patch
from rest_framework.status import HTTP_200_OK

from .utils import create_test_audit_objects
from audit_tool.api.urls.names import AuditPathName
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoVet
from brand_safety.models import BadWordCategory
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from segment.models import CustomSegment
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


@patch("audit_tool.api.views.audit_vet_retrieve_update.generate_vetted_segment")
class AuditVetESUpdateTestCase(ExtendedAPITestCase, ESTestCase):
    channel_manager = ChannelManager(sections=(Sections.BRAND_SAFETY, Sections.TASK_US_DATA, Sections.GENERAL_DATA))
    video_manager = VideoManager(sections=(Sections.BRAND_SAFETY, Sections.TASK_US_DATA, Sections.GENERAL_DATA))

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user(perms={
            StaticPermissions.BUILD__CTL_VET: True,
        })

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        create_test_audit_objects()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def _create_audit_meta_vet(self, audit_type, item_id):
        BadWordCategory.objects.get_or_create(id=1, defaults=dict(name="test_category_1"))
        BadWordCategory.objects.get_or_create(id=2, defaults=dict(name="test_category_2"))
        audit = AuditProcessor.objects.create(audit_type=1)
        if audit_type == "channel":
            audit_item = AuditChannel.objects.create(channel_id=item_id)
            meta = AuditChannelMeta.objects.create(channel=audit_item, name=f"test channel meta name {audit_item.id}")
            vetting_item = AuditChannelVet.objects.create(audit=audit, channel=audit_item)
        else:
            audit_item = AuditVideo.objects.create(video_id=item_id)
            meta = AuditVideoMeta.objects.create(video=audit_item, name=f"test video meta name {audit_item.id}")
            vetting_item = AuditVideoVet.objects.create(audit=audit, video=audit_item)
        return audit_item, meta, vetting_item

    def _get_url(self, kwargs):
        url = reverse(AuditPathName.AUDIT_VET, [Namespace.AUDIT_TOOL], kwargs=kwargs)
        return url

    def test_update_channel_es(self, mock_generate_vetted):
        """ Test vetting updates brand safety and general data """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=1 for channels
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=1, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        bs_category_data = {
            "1": {
                "severity_counts": {
                    "1": 2,
                    "2": 6,
                    "4": 1
                },
                "keywords": ["test", "keyword"],
                "category_score": 43
            },
            "2": {
                "severity_counts": {
                    "1": 7,
                    "2": 11,
                    "4": 3
                },
                "keywords": ["another"],
                "category_score": 5
            },
        }
        channel = Channel(audit_item_yt_id)
        channel.populate_general_data(iab_categories=["some", "wrong", "categories"])
        channel.populate_task_us_data(iab_categories=["more", "wrong"])
        channel.populate_brand_safety(
            videos_scored=1,
            language="en",
            categories=bs_category_data
        )
        self.channel_manager.upsert([channel])
        audit_item, _, vetting_item = self._create_audit_meta_vet("channel", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 2,
            "brand_safety": [
                1
            ],
            "content_type": 1,
            "content_quality": 1,
            "gender": 2,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries"
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=1, defaults=dict(name="test_category_1"))
        BadWordCategory.objects.get_or_create(id=2, defaults=dict(name="test_category_2"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        channel_brand_safety = channel.brand_safety.categories
        updated_channel_brand_safety = updated_channel.brand_safety.categories

        self.assertEqual(channel_brand_safety["1"]["severity_counts"],
                         updated_channel_brand_safety["1"]["severity_counts"])
        self.assertEqual(channel_brand_safety["1"]["keywords"], updated_channel_brand_safety["1"]["keywords"])
        self.assertEqual(channel_brand_safety["2"]["severity_counts"],
                         updated_channel_brand_safety["2"]["severity_counts"])
        self.assertEqual(channel_brand_safety["2"]["keywords"], updated_channel_brand_safety["2"]["keywords"])

        self.assertNotEqual(channel.general_data["iab_categories"], updated_channel.general_data["iab_categories"])
        self.assertNotEqual(channel.task_us_data["iab_categories"], updated_channel.general_data["iab_categories"])
        self.assertEqual(sorted(payload["iab_categories"] + [payload["primary_category"]]),
                         sorted(updated_channel.general_data["iab_categories"]))
        self.assertEqual(sorted(payload["iab_categories"] + [payload["primary_category"]]),
                         sorted(updated_channel.task_us_data["iab_categories"]))

    def test_update_video_es(self, mock_generate_vetted):
        """ Test vetting updates brand safety """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=0 for videos
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=0, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_video_id{next(int_iterator)}"
        bs_category_data = {
            "3": {
                "severity_counts": {
                    "1": 2,
                    "2": 4,
                    "4": 31
                },
                "keywords": ["test", "keyword"],
                "category_score": 5
            },
            "4": {
                "severity_counts": {
                    "1": 1,
                    "2": 2,
                    "4": 3
                },
                "keywords": ["another"],
                "category_score": 56
            },
        }
        video = Video(audit_item_yt_id)
        video.populate_brand_safety(categories=bs_category_data)
        video.populate_general_data(iab_categories=["wrong", "categories"])
        self.video_manager.upsert([video])
        audit_item, _, vetting_item = self._create_audit_meta_vet("video", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            "brand_safety": [
                4
            ],
            "content_type": 1,
            "content_quality": 1,
            "gender": 2,
            "iab_categories": [
                "Automotive", "Scooters", "Auto Rentals", "Industries"
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ko",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=3, defaults=dict(name="test_category_3"))
        BadWordCategory.objects.get_or_create(id=4, defaults=dict(name="test_category_4"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        updated_video = self.video_manager.get([video.main.id])[0]
        video_brand_safety = video.brand_safety.categories
        updated_video_brand_safety = updated_video.brand_safety.categories

        self.assertEqual(video_brand_safety["3"]["severity_counts"],
                         updated_video_brand_safety["3"]["severity_counts"])
        self.assertEqual(video_brand_safety["3"]["keywords"], updated_video_brand_safety["3"]["keywords"])
        self.assertEqual(video_brand_safety["4"]["severity_counts"],
                         updated_video_brand_safety["4"]["severity_counts"])
        self.assertEqual(video_brand_safety["4"]["keywords"], updated_video_brand_safety["4"]["keywords"])

        self.assertNotEqual(video.general_data["iab_categories"], updated_video.general_data["iab_categories"])
        self.assertEqual(sorted(payload["iab_categories"] + [payload["primary_category"]]),
                         sorted(updated_video.general_data["iab_categories"]))

    def test_send_empty_brand_safety_channel_success(self, mock_generate_vetted):
        """ Test sending empty vetted brand safety categories saves properly """
        audit = AuditProcessor.objects.create(audit_type=1)
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=1, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        BadWordCategory.objects.get_or_create(id=5, defaults=dict(name="test_category_5"))
        BadWordCategory.objects.get_or_create(id=6, defaults=dict(name="test_category_6"))
        channel = Channel(f"test_youtube_channel_id{next(int_iterator)}")
        channel.populate_task_us_data(brand_safety=[5, 6])
        self.channel_manager.upsert([channel])
        audit_item, _, vetting_item = self._create_audit_meta_vet("channel", channel.main.id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            "brand_safety": [],  # Should update with empty list
            "content_type": 1,
            "content_quality": 1,
            "primary_category": "Automotive",
            "gender": 2,
            "iab_categories": ["Automotive"],
            "is_monetizable": False,
            "language": "en",
            "suitable": True
        }
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual(payload["iab_categories"] + [payload["primary_category"]],
                         [val for val in updated_channel.task_us_data["iab_categories"] if val is not None])
        self.assertEqual(payload["age_group"], int(updated_channel.task_us_data["age_group"]))
        self.assertEqual(payload["content_type"], int(updated_channel.task_us_data["content_type"]))
        self.assertEqual(payload["gender"], int(updated_channel.task_us_data["gender"]))
        self.assertEqual(payload["language"], updated_channel.task_us_data["lang_code"])

        # Remove None values
        updated_task_us_brand_safety= [val for val in updated_channel.task_us_data["brand_safety"] if val is not None]
        self.assertEqual(payload["brand_safety"], updated_task_us_brand_safety)

    def test_send_empty_brand_safety_videos_success(self, mock_generate_vetted):
        """ Test sending empty vetted brand safety categories saves properly """
        audit = AuditProcessor.objects.create(audit_type=1)
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=0, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        BadWordCategory.objects.get_or_create(id=7, defaults=dict(name="test_category_5"))
        BadWordCategory.objects.get_or_create(id=8, defaults=dict(name="test_category_6"))
        video = Video(f"test_youtube_video_id{next(int_iterator)}")
        video.populate_task_us_data(brand_safety=[7, 8])
        self.channel_manager.upsert([video])
        audit_item, _, vetting_item = self._create_audit_meta_vet("video", video.main.id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            "brand_safety": [],  # Should update with empty list
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "primary_category": "Automotive",
            "iab_categories": ["Automotive"],
            "is_monetizable": False,
            "language": "en",
            "suitable": True
        }
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        updated_video = self.video_manager.get([video.main.id])[0]
        self.assertEqual(payload["iab_categories"] + [payload["primary_category"]],
                         [val for val in updated_video.task_us_data["iab_categories"] if val is not None])
        self.assertEqual(payload["age_group"], int(updated_video.task_us_data["age_group"]))
        self.assertEqual(payload["content_type"], int(updated_video.task_us_data["content_type"]))
        self.assertEqual(payload["gender"], int(updated_video.task_us_data["gender"]))
        self.assertEqual(payload["language"], updated_video.task_us_data["lang_code"])

        # Parse out None values
        updated_task_us_bs_categories = [val for val in updated_video.task_us_data["brand_safety"] if val is not None]
        self.assertEqual(payload["brand_safety"], updated_task_us_bs_categories)

    def test_update_channel_duplicate_categories(self, mock_generate_vetted):
        """ Test vetting updates brand safety and iab categories with no duplicates """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=1 for channels
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=1, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        channel = Channel(audit_item_yt_id)
        channel.populate_general_data(iab_categories=["some", "wrong", "categories"])
        channel.populate_task_us_data(iab_categories=["more", "wrong"])
        channel.populate_brand_safety(
            videos_scored=1,
            language="en",
        )
        self.channel_manager.upsert([channel])
        audit_item, _, vetting_item = self._create_audit_meta_vet("channel", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 2,
            "brand_safety": [
                1, 1,
            ],
            "primary_category": "Automotive",
            "content_type": 1,
            "content_quality": 1,
            "gender": 2,
            "iab_categories": [
                "Automotive", "Auto Rentals", "Scooters", "Auto Rentals",
            ],
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=1, defaults=dict(name="test_category_1"))
        BadWordCategory.objects.get_or_create(id=2, defaults=dict(name="test_category_2"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
            "audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.update_brand_safety") as mock_update_brand_safety:
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual({"1"}, set(updated_channel.task_us_data.brand_safety))
        self.assertEqual({"Automotive", "Auto Rentals", "Scooters"}, set(updated_channel.task_us_data.iab_categories))

    def test_update_video_dupliate_categories(self, mock_generate_vetted):
        """ Test vetting updates brand safety and iab categories with no duplicates """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=0 for videos
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=0, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_video_id{next(int_iterator)}"
        video = Video(audit_item_yt_id)
        self.video_manager.upsert([video])
        audit_item, _, vetting_item = self._create_audit_meta_vet("video", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            "brand_safety": [
                4, 4
            ],
            "content_type": 1,
            "content_quality": 1,
            "gender": 2,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries", "Industries"
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ko",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=3, defaults=dict(name="test_category_A"))
        BadWordCategory.objects.get_or_create(id=4, defaults=dict(name="test_category_B"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        updated_video = self.video_manager.get([video.main.id])[0]
        self.assertEqual({"4"}, set(updated_video.task_us_data.brand_safety))
        self.assertEqual({"Automotive", "Scooters", "Auto Rentals", "Industries"}, set(updated_video.task_us_data.iab_categories))

    def test_patch_admin_channel(self, mock_generate_vetted):
        """ Test admin vetting is final and resolves limbo_status """
        user = self.create_test_user(perms={
            StaticPermissions.BUILD__CTL_VET_ADMIN: True,
        })
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=1 for channels
        CustomSegment.objects.create(owner=user, title="test", segment_type=1, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        channel = Channel(audit_item_yt_id)
        channel.populate_general_data(iab_categories=["some", "wrong", "categories"])
        channel.populate_task_us_data(iab_categories=["more", "wrong"])
        channel.populate_brand_safety(
            videos_scored=1,
            language="en",
            overall_score=90,
            limbo_status=True,
            pre_limbo_score=9,
        )
        self.channel_manager.upsert([channel])
        audit_item, _, vetting_item = self._create_audit_meta_vet("channel", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            # Saving with brand safety marks as unsafe
            "brand_safety": [
                1,
            ],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Automotive", "Auto Rentals", "Scooters", "Auto Rentals",
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=1, defaults=dict(name="test_category_1"))
        BadWordCategory.objects.get_or_create(id=2, defaults=dict(name="test_category_2"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.update_brand_safety"
        ) as mock_update_brand_safety:
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual(updated_channel.brand_safety.limbo_status, False)

    def test_patch_admin_video(self, mock_generate_vetted):
        """ Test admin vetting is final and resolves limbo_status """
        user = self.create_test_user(perms={
            StaticPermissions.BUILD__CTL_VET_ADMIN: True,
        })
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=0 for videos
        CustomSegment.objects.create(owner=user, title="test", segment_type=0, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_video_id{next(int_iterator)}"
        video = Video(
            main=dict(id=audit_item_yt_id),
            brand_safety=dict(overall_score=1)
        )
        self.video_manager.upsert([video])
        audit_item, _, vetting_item = self._create_audit_meta_vet("video", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            # Saving with no brand safety marks as safe
            "brand_safety": [],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries", "Industries"
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ko",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=3, defaults=dict(name=f"test_category_{next(int_iterator)}"))
        BadWordCategory.objects.get_or_create(id=4, defaults=dict(name=f"test_category_{next(int_iterator)}"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer.update_brand_safety") as mock_update_brand_safety:
            self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_video = self.video_manager.get([video.main.id])[0]
        self.assertEqual(updated_video.brand_safety.limbo_status, False)

    def test_patch_channel_review_safe(self, mock_generate_vetted):
        """ Test vetting something as safe with a bad score marks for review and saves previous system score """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=1 for channels
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=1, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        channel = Channel(audit_item_yt_id)
        channel.populate_general_data(iab_categories=["some", "wrong", "categories"])
        channel.populate_task_us_data(iab_categories=["more", "wrong"])
        channel.populate_brand_safety(
            videos_scored=1,
            language="en",
            overall_score=78,
        )
        self.channel_manager.upsert([channel])
        audit_item, _, vetting_item = self._create_audit_meta_vet("channel", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            "brand_safety": [],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Auto Rentals", "Scooters", "Auto Rentals",
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=1, defaults=dict(name="test_category_1"))
        BadWordCategory.objects.get_or_create(id=2, defaults=dict(name="test_category_2"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.update_brand_safety") as mock_update_brand_safety:
            response = self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual(updated_channel.brand_safety.limbo_status, True)
        self.assertEqual(updated_channel.brand_safety.pre_limbo_score, updated_channel.brand_safety.overall_score)

    def test_patch_video_review_unsafe(self, mock_generate_vetted):
        """ Test vetting something as safe with an unsafe score marks for review and saves previous system score """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=0 for videos
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=0, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_video_id{next(int_iterator)}"
        video = Video(
            main=dict(id=audit_item_yt_id),
            brand_safety=dict(overall_score=1)
        )
        self.video_manager.upsert([video])
        audit_item, _, vetting_item = self._create_audit_meta_vet("video", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            # Saving with no brand safety marks as safe
            "brand_safety": [],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries", "Industries"
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ko",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=3, defaults=dict(name=f"test_category_{next(int_iterator)}"))
        BadWordCategory.objects.get_or_create(id=4, defaults=dict(name=f"test_category_{next(int_iterator)}"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer.update_brand_safety") as mock_update_brand_safety:
            self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_video = self.video_manager.get([video.main.id])[0]
        self.assertEqual(updated_video.brand_safety.limbo_status, True)
        self.assertEqual(updated_video.brand_safety.overall_score, video.brand_safety.overall_score)

    def test_patch_channel_confirm_safe(self, mock_generate_vetted):
        """ Test vetting confirm as safe with safe score removes review status by saving limbo_status = False"""
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=1 for channels
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=1, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        channel = Channel(audit_item_yt_id)
        channel.populate_general_data(iab_categories=["some", "wrong", "categories"])
        channel.populate_task_us_data(iab_categories=["more", "wrong"])
        channel.populate_brand_safety(
            videos_scored=1,
            language="en",
            overall_score=90,
            limbo_status=True,
            pre_limbo_score=90,
        )
        self.channel_manager.upsert([channel])
        audit_item, _, vetting_item = self._create_audit_meta_vet("channel", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            # Saving with no brand safety marks as safe
            "brand_safety": [],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Auto Rentals", "Scooters", "Auto Rentals",
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=1, defaults=dict(name="test_category_1"))
        BadWordCategory.objects.get_or_create(id=2, defaults=dict(name="test_category_2"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.update_brand_safety") as mock_update_brand_safety:
            self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual(updated_channel.brand_safety.limbo_status, False)

    def test_patch_video_confirms_unsafe(self, mock_generate_vetted):
        """ Test vetting confirms as unsafe with unsafe score removes review status by saving limbo_status = False"""
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=0 for videos
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=0, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_video_id{next(int_iterator)}"
        video = Video(
            main=dict(id=audit_item_yt_id),
            brand_safety=dict(overall_score=1, limbo_status=True, pre_limbo_score=1)
        )
        self.video_manager.upsert([video])
        audit_item, _, vetting_item = self._create_audit_meta_vet("video", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            # Saving with brand safety marks as unsafe
            "brand_safety": [1],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries", "Industries"
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ko",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=3, defaults=dict(name=f"test_category_{next(int_iterator)}"))
        BadWordCategory.objects.get_or_create(id=4, defaults=dict(name=f"test_category_{next(int_iterator)}"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer.update_brand_safety") as mock_update_brand_safety:
            self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_video = self.video_manager.get([video.main.id])[0]
        self.assertEqual(updated_video.brand_safety.limbo_status, False)

    def test_patch_video_unsafe_ignore(self, mock_generate_vetted):
        """ Test vetting something as unsafe with an safe score does not save limbo status """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=0 for videos
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=0, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_video_id{next(int_iterator)}"
        video = Video(
            main=dict(id=audit_item_yt_id),
            brand_safety=dict(overall_score=100)
        )
        self.video_manager.upsert([video])
        audit_item, _, vetting_item = self._create_audit_meta_vet("video", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            # Saving with no brand safety marks as safe
            "brand_safety": [1],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries", "Industries"
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ko",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=3, defaults=dict(name=f"test_category_{next(int_iterator)}"))
        BadWordCategory.objects.get_or_create(id=4, defaults=dict(name=f"test_category_{next(int_iterator)}"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_video_vet_serializer.AuditVideoVetSerializer.update_brand_safety") as mock_update_brand_safety:
            self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_video = self.video_manager.get([video.main.id])[0]
        self.assertEqual(updated_video.brand_safety.limbo_status, None)

    def test_patch_channel_unsafe_ignore(self, mock_generate_vetted):
        """ Test vetting something as unsafe with an safe score does not save limbo status """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=1 for channels
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=1, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())
        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        channel = Channel(audit_item_yt_id)
        channel.populate_general_data(iab_categories=["some", "wrong", "categories"])
        channel.populate_task_us_data(iab_categories=["more", "wrong"])
        channel.populate_brand_safety(
            videos_scored=1,
            language="en",
            overall_score=90,
        )
        self.channel_manager.upsert([channel])
        audit_item, _, vetting_item = self._create_audit_meta_vet("channel", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            # Saving with no brand safety marks as safe
            "brand_safety": [1],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Auto Rentals", "Scooters", "Auto Rentals",
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=1, defaults=dict(name="test_category_1"))
        BadWordCategory.objects.get_or_create(id=2, defaults=dict(name="test_category_2"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.update_brand_safety") as mock_update_brand_safety:
            self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual(updated_channel.brand_safety.limbo_status, None)

    def test_patch_ignore_unvettable(self, mock_generate_vetted):
        """ Test ignore saving non vettable brand safety categories """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=1 for channels
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=1, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())

        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        channel = Channel(audit_item_yt_id)
        self.channel_manager.upsert([channel])
        audit_item, _, vetting_item = self._create_audit_meta_vet("channel", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            "brand_safety": [17, 22],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Auto Rentals", "Scooters", "Auto Rentals",
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        nonvettable, _ = BadWordCategory.objects.update_or_create(
            id=payload["brand_safety"][0], defaults=dict(vettable=False, name=f"test {next(int_iterator)}"))
        vettable, _ = BadWordCategory.objects.update_or_create(
            id=payload["brand_safety"][1], defaults=dict(vettable=True, name=f"test {next(int_iterator)}"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.update_brand_safety") as mock_update_brand_safety:
            self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual({str(vettable.id)}, set(updated_channel.task_us_data.brand_safety))

    def test_patch_ignore_non_existant_category(self, mock_generate_vetted):
        """ Test ignore saving non existant brand safety categories (May have been removed) """
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=1 for channels
        CustomSegment.objects.create(owner=self.user, title="test", segment_type=1, audit_id=audit.id,
                                     list_type=1, statistics={"items_count": 1}, uuid=uuid4())

        audit_item_yt_id = f"test_youtube_channel_id{next(int_iterator)}"
        channel = Channel(audit_item_yt_id)
        self.channel_manager.upsert([channel])
        audit_item, _, vetting_item = self._create_audit_meta_vet("channel", audit_item_yt_id)
        payload = {
            "vetting_id": vetting_item.id,
            "age_group": 1,
            # BadWordCategory with 100000 does not exist and should be ignored
            "brand_safety": [17, 100000],
            "content_type": 1,
            "content_quality": 1,
            "gender": 1,
            "iab_categories": [
                "Auto Rentals", "Scooters", "Auto Rentals",
            ],
            "primary_category": "Automotive",
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        exists_and_vettable, _ = BadWordCategory.objects.update_or_create(
            id=payload["brand_safety"][0], defaults=dict(vettable=True, name=f"test {next(int_iterator)}"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        with patch(
                "audit_tool.api.serializers.audit_channel_vet_serializer.AuditChannelVetSerializer.update_brand_safety") as mock_update_brand_safety:
            self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual(str(exists_and_vettable.id), updated_channel.task_us_data["brand_safety"][0])
