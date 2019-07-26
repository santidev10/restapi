import json
from unittest.mock import patch

import requests
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from saas.urls.namespaces import Namespace
from userprofile.permissions import Permissions
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.response import MockResponse
from utils.utittests.reverse import reverse
from video.api.urls.names import Name
from utils.utittests.es_components_patcher import SearchDSLPatcher

from es_components.models.video import Video


class VideoRetrieveUpdateTestSpec(ExtendedAPITestCase):
    @classmethod
    def setUpClass(cls):
        super(VideoRetrieveUpdateTestSpec, cls).setUpClass()
        Permissions.sync_groups()

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
        video_id = "video_id"

        with patch("es_components.managers.video.VideoManager.model.get",
                   return_value=Video(id=video_id, ads_stats={"clicks_count": 100})):

            url = self._get_url(video_id)
            response = self.client.get(url)

            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertIn("ads_stats", response.data)

    def test_404_if_no_video(self):
        self.create_test_user()
        missing_video_id = "some_id"

        with patch("es_components.managers.video.VideoManager.model.get",
                   return_value=None):

            url = self._get_url(missing_video_id)
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
