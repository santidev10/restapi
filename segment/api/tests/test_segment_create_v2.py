import json
from datetime import date
from unittest.mock import patch

from django.core.urlresolvers import reverse
from django.http import QueryDict
from rest_framework.status import HTTP_201_CREATED, HTTP_401_UNAUTHORIZED, \
    HTTP_200_OK, HTTP_400_BAD_REQUEST

from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.models import YTVideoStatistic, AWConnection, \
    Account, AWAccountPermission, Campaign, AdGroup
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import SegmentChannel
from segment.models import SegmentVideo
from segment.models import CustomSegmentFileUpload
from utils.utittests.int_iterator import int_iterator
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentListCreateApiViewTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_LIST,
                       kwargs=dict(segment_type=segment_type))

    def test_reject_bad_request(self):
        self.create_test_user()
        payload = {
            "category": "private",
            "list_type": "whitelist",
        }
        response = self.client.post(
            self._get_url("channel"), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success(self):
        self.create_test_user()
        payload = {
            "category": "private",
            "list_type": "whitelist",
            "title": "I am a whitelist",
            "score_threshold": 100,
            "brand_safety_categories": ["1", "3", "4", "5", "6"],
            "youtube_categories": ["education", "gaming"],
            "languages": ["es"]
        }
        response = self.client.post(
            self._get_url("channel"), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
