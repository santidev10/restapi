import mock

from elasticsearch_dsl import Q
from rest_framework.status import HTTP_200_OK

from video.api.urls.names import Name
from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.reverse import reverse


class VideoAggregationsTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, args):
        url = reverse(Name.VIDEO_LIST, [Namespace.VIDEO], query_params=args)
        return url

    def test_success_authenticated(self):
        """ Test allow authenticated get requests as many parts of the client rely on aggregations as filters """
        self.create_test_user()
        params = dict(
            aggregations=""
        )
        url = self._get_url(params)
        with mock.patch.object(VideoManager, "forced_filters", return_value=Q()):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_video_aggregations(self):
        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })
        manager = VideoManager([Sections.TASK_US_DATA, Sections.STATS, Sections.ADS_STATS, Sections.CAPTIONS,
                                Sections.CUSTOM_CAPTIONS])
        aggregations = ["ads_stats.average_cpm:max", "ads_stats.average_cpm:min", "ads_stats.average_cpv:max",
                        "ads_stats.average_cpv:min", "ads_stats.ctr:max", "ads_stats.ctr:min", "ads_stats.ctr_v:max",
                        "ads_stats.ctr_v:min", "ads_stats.video_quartile_100_rate:max",
                        "ads_stats.video_quartile_100_rate:min", "ads_stats.video_view_rate:max",
                        "ads_stats.video_view_rate:min", "ads_stats:exists", "brand_safety", "flags",
                        "general_data.country_code", "general_data.iab_categories", "general_data.lang_code",
                        "general_data.youtube_published_at:max", "general_data.youtube_published_at:min",
                        "stats.channel_subscribers:max", "stats.channel_subscribers:min", "stats.last_day_views:max",
                        "stats.last_day_views:min", "stats.sentiment", "stats.views:max", "stats.views:min",
                        "task_us_data.age_group", "task_us_data.content_type", "task_us_data.content_quality",
                        "task_us_data.gender", "task_us_data:exists", "task_us_data:missing", "captions:exists",
                        "brand_safety.limbo_status",
                        ]
        manager.upsert([manager.model("test_video")], refresh="wait_for")
        params = dict(
            aggregations=",".join(aggregations)
        )
        url = self._get_url(params)
        with mock.patch.object(VideoManager, "forced_filters", return_value=Q()):
            data = self.client.get(url).data["aggregations"]
        self.assertEqual(set(data), set(aggregations))

