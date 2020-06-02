import json

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.api.urls.names import AuditPathName
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.constants import Sections
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse


class AuditItemTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, doc_id):
        url = reverse(AuditPathName.AUDIT_ITEM, [Namespace.AUDIT_TOOL], kwargs=dict(pk=doc_id))
        return url

    def setUp(self):
        self.channel_manager = ChannelManager(sections=[Sections.TASK_US_DATA, Sections.MONETIZATION])
        self.video_manager = VideoManager(sections=[Sections.TASK_US_DATA, Sections.MONETIZATION])

    def test_unauthorized_get(self):
        self.create_test_user()
        response = self.client.get(self._get_url("test"))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_unauthorized_patch(self):
        self.create_test_user()
        response = self.client.patch(self._get_url("test"))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_get_channel(self):
        self.create_admin_user()
        channel = self.channel_manager.model(f"test_youtube_channel_{next(int_iterator)}")
        channel.populate_monetization(is_monetizable=False)
        channel.populate_task_us_data(
            iab_categories=["Hobbies & Interests"],
            lang_code="en",
            age_group=3,
            content_type=1,
            gender=0,
        )
        self.channel_manager.upsert([channel])
        response = self.client.get(self._get_url(channel.main.id))
        data = response.data
        self.assertEqual(channel.task_us_data.iab_categories, data["iab_categories"])
        self.assertEqual(channel.task_us_data.lang_code, data["language"])
        self.assertEqual(channel.task_us_data.age_group, data["age_group"])
        self.assertEqual(channel.task_us_data.content_type, data["content_type"])
        self.assertEqual(channel.task_us_data.gender, data["gender"])
        self.assertEqual(channel.monetization.is_monetizable, data["is_monetizable"])
        self.assertIsNotNone(data.get("vetting_history"))

    def test_get_video(self):
        self.create_admin_user()
        video = self.video_manager.model(f"video_{next(int_iterator)}")
        video.populate_monetization(is_monetizable=True)
        video.populate_task_us_data(
            iab_categories=["Automobiles"],
            lang_code="es",
            age_group=1,
            content_type=2,
            gender=3,
        )
        self.video_manager.upsert([video])
        response = self.client.get(self._get_url(video.main.id))
        data = response.data
        self.assertEqual(video.task_us_data.iab_categories, data["iab_categories"])
        self.assertEqual(video.task_us_data.lang_code, data["language"])
        self.assertEqual(video.task_us_data.age_group, data["age_group"])
        self.assertEqual(video.task_us_data.content_type, data["content_type"])
        self.assertEqual(video.task_us_data.gender, data["gender"])
        self.assertEqual(video.monetization.is_monetizable, data["is_monetizable"])
        self.assertIsNotNone(data.get("vetting_history"))

    def test_requried_fields(self):
        """ Test required files for patch request """
        self.create_admin_user()
        payload = dict(
            language="en"
        )
        response = self.client.patch(self._get_url("test"), data=json.dumps(payload),
                                     content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_patch_channel(self):
        self.create_admin_user()
        channel = self.channel_manager.model(f"test_youtube_channel_{next(int_iterator)}")
        channel.populate_monetization(is_monetizable=True)
        channel.populate_task_us_data(
            iab_categories=["Books & Literature"],
            lang_code="en",
            age_group=2,
            content_type=2,
            gender=2,
        )
        channel.populate_general_data(
            top_lang_code="en",
            top_language="English",
        )
        self.channel_manager.upsert([channel])
        payload = dict(
            iab_categories=["Business & Finance"],
            language="ru",
            age_group="1",
            content_type="1",
            gender="1",
            is_monetizable=False,
            brand_safety=[],
        )
        response = self.client.patch(self._get_url(channel.main.id), data=json.dumps(payload), content_type="application/json")
        updated = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(updated.task_us_data.iab_categories, payload["iab_categories"])
        self.assertEqual(updated.task_us_data.lang_code, payload["language"])
        self.assertEqual(updated.task_us_data.age_group, payload["age_group"])
        self.assertEqual(updated.task_us_data.content_type, payload["content_type"])
        self.assertEqual(updated.task_us_data.gender, payload["gender"])
        self.assertEqual(updated.monetization.is_monetizable, payload["is_monetizable"])

    def test_patch_video(self):
        self.create_admin_user()
        video = self.video_manager.model(f"video_{next(int_iterator)}")
        video.populate_monetization(is_monetizable=True)
        video.populate_task_us_data(
            iab_categories=["Careers"],
            lang_code="es",
            age_group=3,
            content_type=3,
            gender=3,
        )
        video.populate_general_data(
            lang_code="es",
            language="Spanish",
        )
        self.video_manager.upsert([video])
        payload = dict(
            iab_categories=["Events & Attractions"],
            language="af",
            age_group="1",
            content_type="1",
            gender="1",
            is_monetizable=True,
            brand_safety=[],
        )
        response = self.client.patch(self._get_url(video.main.id), data=json.dumps(payload), content_type="application/json")
        updated = self.video_manager.get([video.main.id])[0]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(updated.task_us_data.iab_categories, payload["iab_categories"])
        self.assertEqual(updated.task_us_data.lang_code, payload["language"])
        self.assertEqual(updated.task_us_data.age_group, payload["age_group"])
        self.assertEqual(updated.task_us_data.content_type, payload["content_type"])
        self.assertEqual(updated.task_us_data.gender, payload["gender"])
        self.assertEqual(updated.monetization.is_monetizable, payload["is_monetizable"])