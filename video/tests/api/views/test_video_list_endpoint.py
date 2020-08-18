import urllib
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
        extra_fields = ("brand_safety_data", "chart_data", "transcript", "blacklist_data", "task_us_data",)
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

    def test_filter_by_single_id(self):
        self.create_admin_user()
        items_to_filter = 1
        videos = [Video(next(int_iterator)) for _ in range(items_to_filter + 1)]
        VideoManager([Sections.GENERAL_DATA]).upsert(videos)

        url = self.get_url(**{"main.id": videos[0].main.id})
        response = self.client.get(url)

        self.assertEqual(items_to_filter, len(response.data["items"]))

    def test_relevancy_score_sorting(self):
        """
        test that results are returned in the correct order when sorting by _score
        """
        self.create_admin_user()
        video_ids = [str(next(int_iterator)) for i in range(2)]
        most_relevant_video_title = "Herp derpsum herp derp sherper herp derp derpus herpus derpus"
        most_relevant_video = Video(**{
            "meta": {
                "id": video_ids[0],
            },
            "general_data": {
                "title": most_relevant_video_title,
                "description": "herp derper repper herpus"
            },
        })
        least_relevant_video = Video(**{
            "meta": {
                "id": video_ids[1],
            },
            "general_data": {
                "title": "Derp sherper perper tee. Derperker zerpus herpy derpus",
                "description": "reeper sherpus lurpy derps herp derp",
            },
        })
        VideoManager(sections=[Sections.GENERAL_DATA]).upsert([
            most_relevant_video,
            least_relevant_video
        ])

        search_term = "herp derp"
        desc_url = self.get_url() + urllib.parse.urlencode({
            "general_data.title": search_term,
            "general_data.description": search_term,
            "sort": "_score:desc",
        })
        desc_response = self.client.get(desc_url)
        desc_items = desc_response.data["items"]
        self.assertEqual(
            desc_items[0]["general_data"]["title"],
            most_relevant_video_title
        )

        asc_url = self.get_url() + urllib.parse.urlencode({
            "general_data.title": search_term,
            "general_data.description": search_term,
            "sort": "_score:asc",
        })
        asc_response = self.client.get(asc_url)
        asc_items = asc_response.data["items"]
        self.assertEqual(
            asc_items[-1]["general_data"]["title"],
            most_relevant_video_title
        )

    def test_video_id_query_param_mutation(self):
        """
        Test that a search on a video id correctly mutates the
        query params to return that video only, even where
        the search term exists in a field that is specified in
        the initial search
        """
        user = self.create_test_user()
        user.add_custom_user_permission("video_list")

        video_ids = [str(next(int_iterator)) for i in range(3)]
        video_one = Video(**{
            "meta": {"id": video_ids[0]},
            "main": {'id': video_ids[0]},
            "general_data": {
                "title": "video whose id we're searching for",
                "description": f"some description."
            }
        })
        video_two = Video(**{
            "meta": {"id": video_ids[1]},
            "main": {'id': video_ids[1]},
            "general_data": {
                "title": "the fox is quick",
                "description": f"some description. {video_ids[0]}"
            }
        })
        video_three = Video(**{
            "meta": {"id": video_ids[2]},
            "main": {'id': video_ids[2]},
            "general_data": {
                "title": "the fox is quick and brown",
                "description": f"some description. {video_ids[0]}"
            }
        })
        sections = [Sections.GENERAL_DATA, Sections.MAIN]
        VideoManager(sections=sections).upsert([video_one, video_two, video_three])

        search_term = video_ids[0]
        url = self.get_url() + urllib.parse.urlencode({
            "general_data.title": search_term,
            "general_data.description": search_term,
        })
        response = self.client.get(url)
        items = response.data['items']
        self.assertEqual(items[0]['main']['id'], video_ids[0])
