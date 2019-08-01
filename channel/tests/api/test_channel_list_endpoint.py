import csv
import json
from unittest.mock import patch

from rest_framework.status import HTTP_200_OK

from channel.api.urls.names import ChannelPathName
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.segment_functionality_mixin import SegmentFunctionalityMixin
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.es_components_patcher import SearchDSLPatcher


class ChannelListTestCase(ExtendedAPITestCase, SegmentFunctionalityMixin):
    url = reverse(ChannelPathName.CHANNEL_LIST, [Namespace.CHANNEL])

    def test_simple_list_works(self):
        with patch("es_components.managers.channel.ChannelManager.search",
                   return_value=SearchDSLPatcher()):
            self.create_admin_user()
            response = self.client.get(self.url)
            self.assertEqual(response.status_code, HTTP_200_OK)

    # def test_export_filters(self):
    #     self.create_admin_user()
    #     response = self.client.post(self.url, json.dumps(dict(filters=dict())), content_type="application/json")
    #     self.assertEqual(response.status_code, HTTP_200_OK)
    #     self.assertEqual(response["Content-Type"], "text/csv")
    #     csv_data = get_data_from_csv_response(response)
    #     headers = next(csv_data)
    #     self.assertEqual(headers, [
    #         "title",
    #         "url",
    #         "country",
    #         "category",
    #         "emails",
    #         "subscribers",
    #         "thirty_days_subscribers",
    #         "thirty_days_views",
    #         "views_per_video",
    #         "sentiment",
    #         "engage_rate",
    #         "last_video_published_at",
    #         "brand_safety_score",
    #         "video_view_rate",
    #         "ctr",
    #         "ctr_v",
    #         "average_cpv",
    #     ])
    #     data = [row for row in csv_data]
    #     self.assertGreaterEqual(len(data), 1)



def get_data_from_csv_response(response):
    return csv.reader((row.decode("utf-8") for row in response.streaming_content))
