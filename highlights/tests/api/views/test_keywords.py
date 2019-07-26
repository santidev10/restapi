from unittest.mock import patch
from urllib import parse

from rest_framework.status import HTTP_200_OK

import singledb.connector
from highlights.api.urls.names import HighlightsNames
from saas.urls.namespaces import Namespace
from utils.utittests.response import MockResponse
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class HighlightKeywordsListApiViewTestCase(ExtendedAPITestCase):
    @patch("singledb.connector.requests")
    def test_list_requests_views_from_sdb(self, requests_mock):
        self.create_test_user()
        requests_mock.get.return_value = MockResponse(json=dict())
        url = reverse(HighlightsNames.KEYWORDS, [Namespace.HIGHLIGHTS],
                      query_params=dict(page=1, sort_by="thirty_days_views"))
        with patch("highlights.api.views.keywords.Connector",
                   new=singledb.connector.SingleDatabaseApiConnector_origin):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        call_url = requests_mock.get.call_args[0][0]
        requests_mock.get.assert_called_once_with(call_url, headers={"Content-Type": "application/json"}, verify=False)
        parsed_url = parse.urlparse(call_url)
        query_params = parse.parse_qs(parsed_url.query)
        fields = query_params.get("fields", [])
        self.assertEqual(len(fields), 1)
        fields = fields[0].split(",")
        self.assertIn("daily_views", fields)
        self.assertIn("weekly_views", fields)
        self.assertIn("thirty_days_views", fields)
        self.assertIn("views", fields)
