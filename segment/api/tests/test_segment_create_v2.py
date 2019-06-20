import json

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
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
            "brand_safety_categories": ["1", "3", "4", "5", "6"],
            "languages": ["es"],
            "list_type": "whitelist",
            "score_threshold": 100,
            "title": "I am a whitelist",
            "youtube_categories": ["education", "gaming"]
        }
        response = self.client.post(
            self._get_url("channel"), json.dumps(payload), content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
