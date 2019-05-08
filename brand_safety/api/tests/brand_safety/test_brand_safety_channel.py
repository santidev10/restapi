from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from utils.utittests.response import MockResponse
from utils.utittests.test_case import ExtendedAPITestCase


class BrandSafetyChannelApiViewTestCase(ExtendedAPITestCase):
    @patch("singledb.connector.requests")
    # @patch.object(ElasticSearchConnector, "search_by_id")
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
        requests_mock.get.return_value = MockResponse(json=dict(
            items=[{
                "video_id": "test",
                "title": "test",
                "transcript": None,
                "thumbnail_image_url": "test"
            }]
        ))
        url = reverse(
            "brand_safety_api_urls:brand_safety_channel",
            kwargs={"pk": "test"}
        )
        response = self.client.get(url)
        response_keys = {
            "brand_safety",
            "total_videos_scored",
            "flagged_videos",
            "total_flagged_videos",
        }
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()), response_keys)

