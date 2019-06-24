from unittest.mock import patch

from django.core.urlresolvers import reverse
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentCreationOptionsApiViewTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_CREATION_OPTIONS,
                       kwargs=dict(segment_type=segment_type))

    @patch("brand_safety.utils.BrandSafetyQueryBuilder.execute")
    def test_success(self, es_mock):
        self.create_test_user()
        es_mock.return_value = {
            "took": 5,
            "timed_out": False,
            "_shards": {
                "total": 5,
                "successful": 5,
                "skipped": 0,
                "failed": 0
            },
            "hits": {
                "total": 602411,
                "max_score": None,
                "hits": []
            }
        }
        query_prams = QueryDict(
            "brand_safety_categories=1,2,3&languages=es&list_type=whitelist&score_threshold=50"
        ).urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("channel"), query_prams))
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_reject_invalid_params(self):
        self.create_test_user()
        query_params = QueryDict("country=us").urlencode()
        response = self.client.get(
            "{}?{}".format(self._get_url("video"), query_params))
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
