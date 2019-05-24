from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from utils.utittests.response import MockResponse
from utils.utittests.test_case import ExtendedAPITestCase


class BrandSafetyChannelApiViewTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.sdb_return_value = MockResponse(json=dict(
            items=[{
                "video_id": "test",
                "title": "test",
                "transcript": None,
                "thumbnail_image_url": "test"
            }]
        ))
        self.response_keys = {
            "brand_safety",
            "items",
            "current_page",
            "items",
            "items_count",
            "max_page"
        }

    @patch("singledb.connector.requests")
    @patch("utils.elasticsearch.ElasticSearchConnector.search_by_id")
    def test_brand_safety_channel_with_es_sdb(self, es_mock, requests_mock):
        self.create_test_user()
        es_mock.side_effect = [
            {
                "channel_id": "test",
                "overall_score": 100,
                "videos_scored": 1,
                "categories": {}
            },
            {
                "test": {
                    "video_id": "test",
                    "overall_score": 100,
                    "videos_scored": 1,
                    "categories": {}
                }
            },
        ]
        requests_mock.get.return_value = self.sdb_return_value
        url = reverse(
            "brand_safety_api_urls:brand_safety_channel",
            kwargs={"pk": "test"}
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()), self.response_keys)

    @patch("utils.elasticsearch.ElasticSearchConnector.search_by_id")
    def test_brand_safety_channel_not_found(self, es_mock):
        self.create_test_user()
        es_mock.return_value = {}
        url = reverse(
            "brand_safety_api_urls:brand_safety_channel",
            kwargs={"pk": "test"}
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    @patch("singledb.connector.requests")
    @patch("utils.elasticsearch.ElasticSearchConnector.search_by_id")
    def test_brand_safety_channel_with_es_sdb_threshold(self, es_mock, requests_mock):
        self.create_test_user()
        es_mock.side_effect = [
            {
                "channel_id": "test1",
                "overall_score": 100,
                "videos_scored": 1,
                "categories": {}
            },
            {
                "test_1": {
                    "video_id": "test",
                    "overall_score": 100,
                    "videos_scored": 1,
                    "categories": {}
                },
                "test_2": {
                    "video_id": "test",
                    "overall_score": 50,
                    "videos_scored": 1,
                    "categories": {}
                }
            },
            {
                "channel_id": "test2",
                "overall_score": 100,
                "videos_scored": 1,
                "categories": {}
            },
            {
                "test_1": {
                    "video_id": "test",
                    "overall_score": 100,
                    "videos_scored": 1,
                    "categories": {}
                },
                "test_2": {
                    "video_id": "test",
                    "overall_score": 50,
                    "videos_scored": 1,
                    "categories": {}
                }
            },
        ]
        requests_mock.get.return_value = self.sdb_return_value
        url_1 = "{}?threshold=100".format(reverse("brand_safety_api_urls:brand_safety_channel", kwargs={"pk": "test"}))
        url_2 = "{}?threshold=50".format(reverse("brand_safety_api_urls:brand_safety_channel", kwargs={"pk": "test"}))
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

