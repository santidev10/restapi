from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.api.urls.names import AuditPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class BlocklistListCreateTestCase(ExtendedAPITestCase, ESTestCase):
    SECTIONS = (Sections.BRAND_SAFETY, Sections.TASK_US_DATA, Sections.GENERAL_DATA, Sections.CUSTOM_PROPERTIES)
    channel_manager = ChannelManager(SECTIONS)
    video_manager = VideoManager(SECTIONS)

    def _get_url_video(self):
        url = reverse(AuditPathName.BLOCKLIST_VIDEO_EXPORT, [Namespace.AUDIT_TOOL])
        return url

    def _get_url_channel(self):
        url = reverse(AuditPathName.BLOCKLIST_CHANNEL_EXPORT, [Namespace.AUDIT_TOOL])
        return url

    def test_admin_permission(self):
        self.create_test_user()
        res1 = self.client.get(self._get_url_video())
        res2 = self.client.get(self._get_url_channel())
        self.assertEqual(res1.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(res2.status_code, HTTP_403_FORBIDDEN)

    def test_success(self):
        self.create_admin_user()
        res1 = self.client.get(self._get_url_video())
        res2 = self.client.get(self._get_url_channel())
        self.assertEqual(res1.status_code, HTTP_200_OK)
        self.assertEqual(res2.status_code, HTTP_200_OK)
        self.assertIsNotNone(res1.data["message"])
        self.assertIsNotNone(res2.data["message"])
