import mock

from elasticsearch_dsl import Q

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

    def setUp(self):
        channel_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS))
        video_manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS))
        channel_id_1 = str(next(int_iterator))
        channel_id_2 = str(next(int_iterator))
        channel_id_3 = str(next(int_iterator))
        channel_id_4 = str(next(int_iterator))
        video_id_1 = str(next(int_iterator))
        video_id_2 = str(next(int_iterator))
        video_id_3 = str(next(int_iterator))
        video_id_4 = str(next(int_iterator))
        # Create Test Channels
        channel_1 = Channel(**{
            "meta": {
                "id": channel_id_1
            },
            "general_data": {
                "iab_categories": ["Automotive"]
            },
            "stats": {
                "last_30day_subscribers": 100,
                "last_30day_views": 100
            },
            "ads_stats": {
                "video_view_rate": 100,
                "ctr_v": 1
            }
        })

        # channels = [channel_1, channel_2, channel_3, channel_4]
        # channel_manager.upsert(channels)
        # Create Test Videos
        video_1 = Video(**{
            "meta": {
                "id": video_id_1
            },
            "general_data": {
                "iab_categories": ["Social"]
            },
            "stats": {
                "last_30day_views": 100
            },
            "ads_stats": {
                "video_view_rate": 100,
                "ctr_v": 1
            }
        })

        # videos = [video_1, video_2, video_3, video_4]
        # videos_manager.upsert(videos)

    def test_success(self):
        Permissions.sync_groups()
        user = self.create_test_user()
        user.add_custom_user_group(PermissionGroupNames.RESEARCH)
