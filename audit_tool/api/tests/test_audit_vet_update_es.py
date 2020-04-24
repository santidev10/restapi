import json
from mock import patch

from django.utils import timezone
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import RequestError
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
from uuid import uuid4

from audit_tool.api.serializers.audit_channel_vet_serializer import AuditChannelVetSerializer
from audit_tool.api.serializers.audit_video_vet_serializer import AuditVideoVetSerializer
from audit_tool.api.urls.names import AuditPathName
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
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from es_components.tests.utils import ESTestCase
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


@patch("audit_tool.api.views.audit_vet_retrieve_update.generate_vetted_segment")
class ChannelListTestCase(ExtendedAPITestCase, ESTestCase):
    channel_manager = ChannelManager(sections=(Sections.BRAND_SAFETY, Sections.TASK_US_DATA))

    def test_update_channel_es(self, mock_generate_vetted):
        user = self.create_admin_user()
        before = timezone.now()
        audit, segment = self._create_segment_audit(user, segment_params=dict(segment_type=1, title="test_title_1"))
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
        channel.populate_brand_safety(
            videos_scored=1,
            language="en",
            categories=bs_category_data
        )
        self.channel_manager.upsert([channel])

        audit_item = AuditChannel.objects.create(channel_id=audit_item_yt_id)
        AuditChannelMeta.objects.create(channel=audit_item, name="test meta name")
        vetting_item = AuditChannelVet.objects.create(audit=audit, channel=audit_item)
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
        BadWordCategory.objects.create(id=1, name="test_category_1")
        BadWordCategory.objects.create(id=2, name="test_category_2")
        url = self._get_url(kwargs=dict(pk=audit.id))
        self.client.patch(url, data=json.dumps(payload), content_type="application/json")

        updated_channel = self.channel_manager.get(channel.main.id)
        channel_brand_safety = channel.brand_safety.categories
        updated_channel_brand_safety = updated_channel.brand_safety.categories
        self.assertNotEqual(channel_brand_safety["1"]["category_score"], updated_channel_brand_safety["1"]["category_score"])
        self.assertNotEqual(channel_brand_safety["2"]["category_score"], updated_channel_brand_safety["2"]["category_score"])
        self.assertEqual(channel_brand_safety["1"]["category_score"], 0)
        self.assertEqual(updated_channel_brand_safety["2"]["category_score"], 100)
        self.assertEqual(channel_brand_safety["1"]["severity_counts"], updated_channel_brand_safety["1"]["severity_counts"])
        self.assertEqual(channel_brand_safety["1"]["keywords"], updated_channel_brand_safety["1"]["keywords"])
        self.assertEqual(channel_brand_safety["2"]["severity_counts"],updated_channel_brand_safety["2"]["severity_counts"])
        self.assertEqual(channel_brand_safety["2"]["keywords"], updated_channel_brand_safety["2"]["keywords"])
