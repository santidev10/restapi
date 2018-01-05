from unittest.mock import patch
from urllib import parse

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from saas.utils_tests import ExtendedAPITestCase, MockResponse


class HighlightKeywordsListApiViewTestCase(ExtendedAPITestCase):
    @patch("singledb.connector.requests")
    def test_list_requests_views_from_sdb(self, requests_mock):
        self.create_test_user()
        requests_mock.get.return_value = MockResponse(json=dict())
        url = reverse("singledb_api_urls:highlights_keywords") + "?" \
              + parse.urlencode(dict(page=1, sort_by="thirty_days_views"))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        requests_mock.get.assert_called_once()
        call_url = requests_mock.get.call_args[0][0]
        parsed_url = parse.urlparse(call_url)
        query_params = parse.parse_qs(parsed_url.query)
        fields = query_params.get("fields", [])
        self.assertEqual(len(fields), 1)
        fields = fields[0].split(",")
        self.assertIn("daily_views", fields)
        self.assertIn("weekly_views", fields)
        self.assertIn("thirty_days_views", fields)
        self.assertIn("views", fields)
