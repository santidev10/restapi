import mock

from elasticsearch_dsl import Q
from rest_framework.status import HTTP_200_OK

from channel.api.urls.names import ChannelPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.reverse import reverse


class ChannelAggregationsTestCase(ExtendedAPITestCase, ESTestCase):
    def _get_url(self, args):
        url = reverse(ChannelPathName.CHANNEL_LIST, [Namespace.CHANNEL], query_params=args)
        return url

    def test_success_authenticated(self):
        """ Test allow authenticated get requests as many parts of the client rely on aggregations as filters """
        self.create_test_user()
        params = dict(
            aggregations=""
        )
        url = self._get_url(params)
        with mock.patch.object(ChannelManager, "forced_filters", return_value=Q()):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_channel_aggregations(self):
        self.create_admin_user()
        manager = ChannelManager([Sections.TASK_US_DATA, Sections.STATS, Sections.ADS_STATS])
        aggregations = ["ads_stats.average_cpm:max", "ads_stats.average_cpm:min", "ads_stats.average_cpv:max",
                        "ads_stats.average_cpv:min", "ads_stats.ctr:max", "ads_stats.ctr:min", "ads_stats.ctr_v:max",
                        "ads_stats.ctr_v:min", "ads_stats.video_quartile_100_rate:max",
                        "ads_stats.video_quartile_100_rate:min", "ads_stats.video_view_rate:max",
                        "ads_stats.video_view_rate:min", "ads_stats:exists", "analytics.age13_17:max",
                        "analytics.age13_17:min", "analytics.age18_24:max", "analytics.age18_24:min",
                        "analytics.age25_34:max", "analytics.age25_34:min", "analytics.age35_44:max",
                        "analytics.age35_44:min", "analytics.age45_54:max", "analytics.age45_54:min",
                        "analytics.age55_64:max", "analytics.age55_64:min", "analytics.age65_:max",
                        "analytics.age65_:min", "analytics.gender_female:max", "analytics.gender_female:min",
                        "analytics.gender_male:max", "analytics.gender_male:min", "analytics.gender_other:max",
                        "analytics.gender_other:min", "brand_safety", "custom_properties.is_tracked",
                        "custom_properties.preferred", "general_data.country_code", "general_data.iab_categories",
                        "general_data.top_lang_code", "monetization.is_monetizable:exists",
                        "stats.last_30day_subscribers:max", "stats.last_30day_subscribers:min",
                        "stats.last_30day_views:max", "stats.last_30day_views:min", "stats.subscribers:max",
                        "stats.subscribers:min", "stats.views_per_video:max", "stats.views_per_video:min",
                        "task_us_data.age_group", "task_us_data.content_type", "task_us_data.content_quality",
                        "task_us_data.gender", "task_us_data:exists", "task_us_data:missing",
                        "brand_safety.limbo_status",]
        manager.upsert([manager.model("test_channel")], refresh="wait_for")
        params = dict(
            aggregations=",".join(aggregations)
        )
        url = self._get_url(params)
        with mock.patch.object(ChannelManager, "forced_filters", return_value=Q()):
            data = self.client.get(url).data["aggregations"]
        self.assertEqual(set(data), set(aggregations))

