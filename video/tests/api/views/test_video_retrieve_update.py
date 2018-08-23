import json
from unittest.mock import patch

import requests
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase, MockResponse
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
from utils.utils_tests import reverse
from video.api.urls.names import Name


class VideoRetrieveUpdateTestSpec(ExtendedAPITestCase):
    def _get_url(self, video_id):
        return reverse(
            Name.VIDEO,
            [Namespace.VIDEO],
            args=(video_id,),
        )

    def test_professional_user_should_see_video_aw_data(self):
        """
        Ticket https://channelfactory.atlassian.net/browse/SAAS-1695
        """
        user = self.create_test_user(True)

        self.fill_all_groups(user)

        with open('saas/fixtures/singledb_video_list.json') as data_file:
            data = json.load(data_file)
        video_id = data["items"][0]["id"]

        url = self._get_url(video_id)
        with patch("video.api.views.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("aw_data", response.data)

    def test_404_if_no_video(self):
        self.create_test_user()
        missing_video_id = "some_id"

        url = self._get_url(missing_video_id)
        with patch.object(requests, "get", return_value=MockResponse(HTTP_404_NOT_FOUND)):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
