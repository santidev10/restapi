from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from utils.utittests.test_case import ExtendedAPITestCase


class BrandSafetyVideoApiViewTestCase(ExtendedAPITestCase):
    @patch("utils.elasticsearch.ElasticSearchConnector.search_by_id")
    def test_brand_safety_video(self, es_mock):
        self.create_test_user()
        es_mock.return_value = {
            "video_id": "test",
            "overall_score": 100,
            "categories": {}
        }
        url = reverse(
            "brand_safety_api_urls:brand_safety_video",
            kwargs={"pk": "test"}
        )
        response = self.client.get(url)
        response_keys = {
            "score",
            "label",
            "total_unique_flagged_words",
            "category_flagged_words",
            "worst_words"
        }
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()), response_keys)

    @patch("utils.elasticsearch.ElasticSearchConnector.search_by_id")
    def test_brand_safety_video_not_found(self, es_mock):
        self.create_test_user()
        es_mock.return_value = {}
        url = reverse(
            "brand_safety_api_urls:brand_safety_video",
            kwargs={"pk": "test"}
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)