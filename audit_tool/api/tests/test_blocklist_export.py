import json
from mock import patch

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.api.urls.names import AuditPathName
from audit_tool.models import BlacklistItem
from audit_tool.models import get_hash_name
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class BlocklistListCreateTestCase(ExtendedAPITestCase, ESTestCase):
    SECTIONS = (Sections.BRAND_SAFETY, Sections.TASK_US_DATA, Sections.GENERAL_DATA, Sections.CUSTOM_PROPERTIES)
    channel_manager = ChannelManager(SECTIONS)
    video_manager = VideoManager(SECTIONS)

    def _get_url(self, data_type):
        url = reverse(AuditPathName.BLOCKLIST_EXPORT, [Namespace.AUDIT_TOOL], kwargs=dict(data_type=data_type))
        return url

    def test_admin_permission(self):
        self.create_test_user()
        res1 = self.client.get(self._get_url("video"))
        res2 = self.client.get(self._get_url("channel"))
        self.assertEqual(res1.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(res2.status_code, HTTP_403_FORBIDDEN)

    def test_success(self):
        self.create_admin_user()
        res = self.client.get(self._get_url("video"))
        self.assertEqual(res.status_code, HTTP_200_OK)
        self.assertIsNotNone(res.data["message"])
