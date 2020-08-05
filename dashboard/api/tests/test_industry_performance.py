from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from dashboard.api.urls.names import DashboardPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.permissions import Permissions
from userprofile.permissions import PermissionGroupNames
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.reverse import reverse


class DashBoardIndustryPerformanceTestCase(ExtendedAPITestCase, ESTestCase):
    _url = reverse(DashboardPathName.DASHBOARD_INDUSTRY_PERFORMANCE, [Namespace.DASHBOARD])

    @classmethod
    def setUpTestData(cls):
        channel_manager = ChannelManager(sections=(Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS,
                                                   Sections.ADS_STATS))
        video_manager = VideoManager(sections=(Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS))
        channel_id_1 = 1
        channel_id_2 = 2
        channel_id_3 = 3
        channel_id_4 = 4
        video_id_1 = 5
        video_id_2 = 6
        video_id_3 = 7
        video_id_4 = 8

        # Create Test Channels
        channel_1 = Channel(**{
            "main": {
                "id": channel_id_1
            },
            "general_data": {
                "iab_categories": ["Automotive"]
            },
            "stats": {
                "last_30day_subscribers": 100,
                "last_30day_views": 100,
                "total_videos_count": 1
            },
            "ads_stats": {
                "video_view_rate": 75,
                "ctr_v": 0.6
            }
        })
        channel_2 = Channel(**{
            "main": {
                "id": channel_id_2
            },
            "general_data": {
                "iab_categories": ["Automotive"]
            },
            "stats": {
                "last_30day_subscribers": 400,
                "last_30day_views": 400,
                "total_videos_count": 1
            },
            "ads_stats": {
                "video_view_rate": 100,
                "ctr_v": 0.7
            }
        })

        channel_3 = Channel(**{
            "main": {
                "id": channel_id_3
            },
            "general_data": {
                "iab_categories": ["Social"]
            },
            "stats": {
                "last_30day_subscribers": 200,
                "last_30day_views": 200,
                "total_videos_count": 1
            },
            "ads_stats": {
                "video_view_rate": 85,
                "ctr_v": 0.8
            }
        })

        channel_4 = Channel(**{
            "main": {
                "id": channel_id_4
            },
            "general_data": {
                "iab_categories": ["Video Gaming"]
            },
            "stats": {
                "last_30day_subscribers": 50,
                "last_30day_views": 50,
                "total_videos_count": 1
            },
            "ads_stats": {
                "video_view_rate": 50,
                "ctr_v": 0.4
            }
        })
        channels = [channel_1, channel_2, channel_3, channel_4]
        channel_manager.upsert(channels)

        # Create Test Videos
        video_1 = Video(**{
            "main": {
                "id": video_id_1
            },
            "general_data": {
                "iab_categories": ["Automotive"]
            },
            "stats": {
                "last_30day_views": 100
            },
            "ads_stats": {
                "video_view_rate": 75,
                "ctr_v": 0.6
            }
        })
        video_2 = Video(**{
            "main": {
                "id": video_id_2
            },
            "general_data": {
                "iab_categories": ["Automotive"]
            },
            "stats": {
                "last_30day_views": 400
            },
            "ads_stats": {
                "video_view_rate": 30,
                "ctr_v": 1
            }
        })
        video_3 = Video(**{
            "main": {
                "id": video_id_3
            },
            "general_data": {
                "iab_categories": ["Social"]
            },
            "stats": {
                "last_30day_views": 200
            },
            "ads_stats": {
                "video_view_rate": 50,
                "ctr_v": 0.8
            }
        })
        video_4 = Video(**{
            "main": {
                "id": video_id_4
            },
            "general_data": {
                "iab_categories": ["Video Gaming"]
            },
            "stats": {
                "last_30day_views": 50
            },
            "ads_stats": {
                "video_view_rate": 100,
                "ctr_v": 0.4
            }
        })
        videos = [video_1, video_2, video_3, video_4]
        video_manager.upsert(videos)

    def setUp(self):
        self.channel_id_1 = 1
        self.channel_id_2 = 2
        self.channel_id_3 = 3
        self.channel_id_4 = 4
        self.video_id_1 = 5
        self.video_id_2 = 6
        self.video_id_3 = 7
        self.video_id_4 = 8

    def test_success(self):
        Permissions.sync_groups()
        user = self.create_test_user()
        user.add_custom_user_group(PermissionGroupNames.RESEARCH)
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data["top_channels"]), 4)
        self.assertEqual(len(data["top_videos"]), 4)
        self.assertEqual(len(data["top_categories"]), 10)
        self.assertEqual(data["top_channels"][0]["id"], self.channel_id_2)
        self.assertEqual(data["top_videos"][0]["id"], self.video_id_2)
        self.assertEqual(data["top_categories"][0]["key"], "Automotive")
        self.assertEqual(data["top_categories"][1]["key"], "Social")
        self.assertEqual(data["top_categories"][2]["key"], "Video Gaming")

    def test_sorting(self):
        Permissions.sync_groups()
        user = self.create_test_user()
        user.add_custom_user_group(PermissionGroupNames.RESEARCH)
        response = self.client.get(f"{self._url}?channel_sort=ads_stats.ctr_v&video_sort=ads_stats.video_view_rate"
                                   f"&category_sort=ads_stats.ctr_v")
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["top_channels"][0]["id"], self.channel_id_3)
        self.assertEqual(data["top_videos"][0]["id"], self.video_id_4)
        self.assertEqual(data["top_categories"][0]["key"], "Social")

    def test_permissions_fail(self):
        """ Test user must have Tools group permissions """
        self.create_test_user()
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
