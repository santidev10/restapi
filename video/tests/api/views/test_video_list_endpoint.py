from unittest.mock import PropertyMock
from unittest.mock import patch

from rest_framework.status import HTTP_200_OK

from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.api.research import ResearchPaginator
from utils.unittests.es_components_patcher import SearchDSLPatcher
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.segment_functionality_mixin import SegmentFunctionalityMixin
from utils.unittests.test_case import ExtendedAPITestCase
from video.api.urls.names import Name


class VideoListTestCase(ExtendedAPITestCase, SegmentFunctionalityMixin, ESTestCase):
    def get_url(self, **kwargs):
        return reverse(Name.VIDEO_LIST, [Namespace.VIDEO], query_params=kwargs)

    def test_simple_list_works(self):
        self.create_admin_user()
        with patch("es_components.managers.video.VideoManager.search",
                   return_value=SearchDSLPatcher()):
            response = self.client.get(self.get_url())
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_limit_to_list_limit(self):
        self.create_admin_user()
        max_page_number = 1
        page_size = 1
        items = [Video(next(int_iterator)) for _ in range(max_page_number * page_size + 1)]
        VideoManager([Sections.GENERAL_DATA]).upsert(items)

        with patch.object(ResearchPaginator, "max_page_number",
                          new_callable=PropertyMock(return_value=max_page_number)):
            response = self.client.get(self.get_url(size=page_size))

            self.assertEqual(max_page_number, response.data["max_page"])
            self.assertEqual(page_size, len(response.data["items"]))
            self.assertEqual(len(items), response.data["items_count"])

    def test_extra_fields(self):
        self.create_admin_user()
        extra_fields = ("brand_safety_data", "chart_data", "transcript", "blacklist_data")
        video = Video(next(int_iterator))
        VideoManager([Sections.GENERAL_DATA]).upsert([video])

        url = self.get_url()
        response = self.client.get(url)

        for field in extra_fields:
            with self.subTest(field):
                self.assertIn(field, response.data["items"][0])

    def test_filter_by_ids(self):
        self.create_admin_user()
        items_to_filter = 2
        videos = [Video(next(int_iterator)) for _ in range(items_to_filter + 1)]
        VideoManager([Sections.GENERAL_DATA]).upsert(videos)

        url = self.get_url(**{"main.id": ",".join([str(video.main.id) for video in videos[:items_to_filter]])})
        response = self.client.get(url)

        self.assertEqual(items_to_filter, len(response.data["items"]))
