import json
from mock import patch

from uuid import uuid4

from audit_tool.api.urls.names import AuditPathName
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoVet
from brand_safety.models import BadWordCategory
from es_components.models import Channel
from es_components.models import Video
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from segment.models import CustomSegment
from utils.unittests.reverse import reverse

from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


@patch("audit_tool.api.views.audit_vet_retrieve_update.generate_vetted_segment")
class AuditVetESUpdateTestCase(ExtendedAPITestCase, ESTestCase):
    channel_manager = ChannelManager(sections=(Sections.BRAND_SAFETY, Sections.TASK_US_DATA, Sections.GENERAL_DATA))
    video_manager = VideoManager(sections=(Sections.BRAND_SAFETY, Sections.TASK_US_DATA, Sections.GENERAL_DATA))

    def _create_audit_meta_vet(self, audit_type, item_id):
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
        user = self.create_admin_user()
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=1 for channels
        CustomSegment.objects.create(owner=user, title="test", segment_type=1, audit_id=audit.id,
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
                "category_score": 0
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
            "gender": 2,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries"
            ],
            "is_monetizable": False,
            "language": "ja",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=1, defaults=dict(name="test_category_1"))
        BadWordCategory.objects.get_or_create(id=2, defaults=dict(name="test_category_2"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        self.client.patch(url, data=json.dumps(payload), content_type="application/json")

        updated_channel = self.channel_manager.get([channel.main.id])[0]
        channel_brand_safety = channel.brand_safety.categories
        updated_channel_brand_safety = updated_channel.brand_safety.categories
        self.assertNotEqual(channel_brand_safety["1"]["category_score"], updated_channel_brand_safety["1"]["category_score"])
        self.assertNotEqual(channel_brand_safety["2"]["category_score"], updated_channel_brand_safety["2"]["category_score"])
        self.assertEqual(updated_channel_brand_safety["1"]["category_score"], 0)
        self.assertEqual(updated_channel_brand_safety["2"]["category_score"], 100)
        self.assertEqual(channel_brand_safety["1"]["severity_counts"], updated_channel_brand_safety["1"]["severity_counts"])
        self.assertEqual(channel_brand_safety["1"]["keywords"], updated_channel_brand_safety["1"]["keywords"])
        self.assertEqual(channel_brand_safety["2"]["severity_counts"],updated_channel_brand_safety["2"]["severity_counts"])
        self.assertEqual(channel_brand_safety["2"]["keywords"], updated_channel_brand_safety["2"]["keywords"])

        self.assertNotEqual(channel.general_data["iab_categories"], updated_channel.general_data["iab_categories"])
        self.assertNotEqual(channel.task_us_data["iab_categories"], updated_channel.general_data["iab_categories"])
        self.assertEqual(payload["iab_categories"], updated_channel.general_data["iab_categories"])
        self.assertEqual(payload["iab_categories"], updated_channel.task_us_data["iab_categories"])

    def test_update_video_es(self, mock_generate_vetted):
        """ Test vetting updates brand safety """
        user = self.create_admin_user()
        audit = AuditProcessor.objects.create(audit_type=1)
        # CustomSegment segment_type=0 for videos
        CustomSegment.objects.create(owner=user, title="test", segment_type=0, audit_id=audit.id,
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
                "category_score": 100
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
            "gender": 2,
            "iab_categories": [
                "Scooters", "Auto Rentals", "Industries"
            ],
            "is_monetizable": False,
            "language": "ko",
            "suitable": True
        }
        BadWordCategory.objects.get_or_create(id=3, defaults=dict(name="test_category_1"))
        BadWordCategory.objects.get_or_create(id=4, defaults=dict(name="test_category_2"))
        url = self._get_url(kwargs=dict(pk=audit.id))
        self.client.patch(url, data=json.dumps(payload), content_type="application/json")

        updated_video = self.video_manager.get([video.main.id])[0]
        video_brand_safety = video.brand_safety.categories
        updated_video_brand_safety = updated_video.brand_safety.categories
        self.assertNotEqual(video_brand_safety["3"]["category_score"], updated_video_brand_safety["3"]["category_score"])
        self.assertNotEqual(video_brand_safety["4"]["category_score"], updated_video_brand_safety["4"]["category_score"])
        self.assertEqual(updated_video_brand_safety["3"]["category_score"], 100)
        self.assertEqual(updated_video_brand_safety["4"]["category_score"], 0)
        self.assertEqual(video_brand_safety["3"]["severity_counts"], updated_video_brand_safety["3"]["severity_counts"])
        self.assertEqual(video_brand_safety["3"]["keywords"], updated_video_brand_safety["3"]["keywords"])
        self.assertEqual(video_brand_safety["4"]["severity_counts"],updated_video_brand_safety["4"]["severity_counts"])
        self.assertEqual(video_brand_safety["4"]["keywords"], updated_video_brand_safety["4"]["keywords"])

        self.assertNotEqual(video.general_data["iab_categories"], updated_video.general_data["iab_categories"])
        self.assertEqual(payload["iab_categories"], updated_video.general_data["iab_categories"])

    def test_send_empty_brand_safety_channel_success(self, mock_generate_vetted):
        """ Test sending empty vetted brand safety categories saves properly """
        user = self.create_admin_user()
        audit = AuditProcessor.objects.create(audit_type=1)
        CustomSegment.objects.create(owner=user, title="test", segment_type=1, audit_id=audit.id,
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
            "brand_safety": [], # Should update with empty list
            "content_type": 1,
            "gender": 2,
            "iab_categories": [],
            "is_monetizable": False,
            "language": "en",
            "suitable": True
        }
        url = self._get_url(kwargs=dict(pk=audit.id))
        self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual(payload["brand_safety"], [val for val in updated_channel.task_us_data["brand_safety"] if val is not None])
        self.assertEqual(payload["age_group"], int(updated_channel.task_us_data["age_group"]))
        self.assertEqual(payload["content_type"], int(updated_channel.task_us_data["content_type"]))
        self.assertEqual(payload["gender"], int(updated_channel.task_us_data["gender"]))
        self.assertEqual(payload["language"], updated_channel.task_us_data["lang_code"])

        # Remove None values
        updated_bs_categories = [val for val in updated_channel.task_us_data["iab_categories"] if val is not None]
        self.assertEqual(payload["brand_safety"], updated_bs_categories)

    def test_send_empty_brand_safety_videos_success(self, mock_generate_vetted):
        """ Test sending empty vetted brand safety categories saves properly """
        user = self.create_admin_user()
        audit = AuditProcessor.objects.create(audit_type=1)
        CustomSegment.objects.create(owner=user, title="test", segment_type=0, audit_id=audit.id,
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
            "brand_safety": [], # Should update with empty list
            "content_type": 1,
            "gender": 1,
            "iab_categories": [],
            "is_monetizable": False,
            "language": "en",
            "suitable": True
        }
        url = self._get_url(kwargs=dict(pk=audit.id))
        self.client.patch(url, data=json.dumps(payload), content_type="application/json")
        updated_video = self.video_manager.get([video.main.id])[0]
        self.assertEqual(payload["iab_categories"], [val for val in updated_video.task_us_data["iab_categories"] if val is not None])
        self.assertEqual(payload["age_group"], int(updated_video.task_us_data["age_group"]))
        self.assertEqual(payload["content_type"], int(updated_video.task_us_data["content_type"]))
        self.assertEqual(payload["gender"], int(updated_video.task_us_data["gender"]))
        self.assertEqual(payload["language"], updated_video.task_us_data["lang_code"])

        # Parse out None values
        updated_task_us_bs_categories = [val for val in updated_video.task_us_data["brand_safety"] if val is not None]
        self.assertEqual(payload["brand_safety"], updated_task_us_bs_categories)
