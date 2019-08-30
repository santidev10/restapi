from rest_framework.status import HTTP_200_OK

from brand_safety.api.urls.names import BrandSafetyPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BrandSafetyChannelApiViewTestCase(ExtendedAPITestCase, ESTestCase):
    def setUp(self):
        super(BrandSafetyChannelApiViewTestCase, self).setUp()
        self.response_keys = {
            "brand_safety",
            "items",
            "current_page",
            "items",
            "items_count",
            "max_page"
        }

    def test_brand_safety_channel_with_es_sdb(self):
        self.create_test_user()
        channel = Channel("test")
        ChannelManager(Sections.BRAND_SAFETY).upsert([channel])
        url = reverse(
            BrandSafetyPathName.BrandSafety.GET_BRAND_SAFETY_CHANNEL, [Namespace.BRAND_SAFETY],
            kwargs={"pk": channel.main.id}
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()), self.response_keys)

    def test_brand_safety_channel_not_found(self):
        self.create_test_user()
        url = reverse(
            BrandSafetyPathName.BrandSafety.GET_BRAND_SAFETY_CHANNEL, [Namespace.BRAND_SAFETY],
            kwargs={"pk": "test"}
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["brand_safety"]["total_videos_scored"], 0)
        self.assertEqual(response.data["brand_safety"]["total_flagged_videos"], 0)
        self.assertEqual(response.data["brand_safety"]["score"], None)
        self.assertEqual(response.data["brand_safety"]["label"], None)
        self.assertEqual(response.data["current_page"], 1)
        self.assertEqual(response.data["items_count"], 0)
        self.assertEqual(response.data["max_page"], 1)

    def test_brand_safety_channel_with_threshold(self):
        self.create_test_user()
        channel = Channel("test")
        ChannelManager(Sections.BRAND_SAFETY).upsert([channel])
        video_1 = Video("test1")
        video_2 = Video("test2")
        video_1.populate_brand_safety(overall_score=100)
        video_2.populate_brand_safety(overall_score=50)
        video_1.populate_channel(id=channel.main.id)
        video_2.populate_channel(id=channel.main.id)
        VideoManager(sections=(Sections.BRAND_SAFETY, Sections.CHANNEL)).upsert([video_1, video_2])
        url_1 = reverse(
            BrandSafetyPathName.BrandSafety.GET_BRAND_SAFETY_CHANNEL, [Namespace.BRAND_SAFETY],
            query_params=dict(threshold=100),
            kwargs={"pk": channel.main.id},
        )
        url_2 = reverse(
            BrandSafetyPathName.BrandSafety.GET_BRAND_SAFETY_CHANNEL, [Namespace.BRAND_SAFETY],
            query_params=dict(threshold=50),
            kwargs={"pk": channel.main.id},
        )
        response_1 = self.client.get(url_1)
        response_2 = self.client.get(url_2)
        self.assertEqual(response_1.status_code, HTTP_200_OK)
        self.assertEqual(set(response_1.data.keys()), self.response_keys)
        self.assertEqual(len(response_1.data["items"]), 2)
        self.assertEqual(response_1.data["brand_safety"]["total_flagged_videos"], 2)

        self.assertEqual(response_2.status_code, HTTP_200_OK)
        self.assertEqual(set(response_2.data.keys()), self.response_keys)
        self.assertEqual(len(response_2.data["items"]), 1)
        self.assertEqual(response_2.data["brand_safety"]["total_flagged_videos"], 1)
