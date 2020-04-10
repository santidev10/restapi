from unittest.mock import patch

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models.video import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.permissions import Permissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase
from video.api.urls.names import Name


class VideoRetrieveUpdateTestSpec(ExtendedAPITestCase, ESTestCase):
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

    @patch("brand_safety.auditors.utils.AuditUtils.get_items", return_value=[])
    def test_professional_user_should_see_video_aw_data(self, mock_get_items):
        """
        Ticket https://channelfactory.atlassian.net/browse/SAAS-1695
        """
        mock_get_items.return_value = []
        user = self.create_test_user(True)

        self.fill_all_groups(user)
        video_id = "video_id"

        with patch("es_components.managers.video.VideoManager.model.get",
                   return_value=Video(id=video_id, ads_stats={"clicks_count": 100})):
            url = self._get_url(video_id)
            response = self.client.get(url)

            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertIn("ads_stats", response.data)

    @patch("brand_safety.auditors.utils.AuditUtils.get_items", return_value=[])
    def test_user_should_see_chart_data(self, mock_get_items):
        """
        Ticket https://channelfactory.atlassian.net/browse/SAAS-1695
        """
        mock_get_items.return_value = []
        user = self.create_test_user(True)

        self.fill_all_groups(user)
        video_id = "video_id"

        stats = {
            "likes_raw_history": {
                "2020-01-02": 1003,
                "2020-01-09": 1500
            },
            "dislikes_raw_history": {
                "2020-01-02": 300,
                "2020-01-09": 780
            },
            "comments_raw_history": {
                "2020-01-02": 300,
                "2020-01-09": 780
            },
            "views_raw_history": {
                "2020-01-02": 10300,
                "2020-01-09": 30300,
            }
        }

        expected_data = [
            {
                "created_at": "2020-01-02 23:59:59.999999Z",
                "views": 10300,
                "likes": 1003,
                "dislikes": 300,
                "comments": 300
            },
            {
                "created_at": "2020-01-09 23:59:59.999999Z",
                "views": 30300,
                "likes": 1500,
                "dislikes": 780,
                "comments": 780
            },
        ]

        with patch("es_components.managers.video.VideoManager.model.get",
                   return_value=Video(id=video_id, stats=stats)):
            url = self._get_url(video_id)
            response = self.client.get(url)

            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data.get("chart_data"), expected_data)

    def test_404_if_no_video(self):
        self.create_test_user()
        missing_video_id = "some_id"

        with patch("es_components.managers.video.VideoManager.model.get",
                   return_value=None):
            url = self._get_url(missing_video_id)
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_extra_fields(self):
        self.create_admin_user()
        extra_fields = ("brand_safety_data", "chart_data", "transcript", "blacklist_data")
        video = Video(str(next(int_iterator)))
        VideoManager([Sections.GENERAL_DATA]).upsert([video])

        url = self._get_url(video.main.id)
        response = self.client.get(url)

        for field in extra_fields:
            with self.subTest(field):
                self.assertIn(field, response.data)
