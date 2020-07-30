from datetime import timedelta
import json

from rest_framework.response import Response
from rest_framework.views import APIView

from cache.models import CacheItem
from dashboard.api.views.constants import DASHBOARD_INDUSTRY_PERFORMANCE_CACHE_PREFIX
from dashboard.utils import get_cache_key
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from utils.datetime import now_in_default_tz


class DashboardIndustryPerformanceAPIView(APIView):
    CACHE_TTL = 3600
    ALLOWED_CHANNEL_SORTS = ["stats.last_30day_subscribers", "stats.last_30day_views", "ads_stats.video_view_rate",
                             "ads_stats.ctr_v"]
    ALLOWED_VIDEO_SORTS = ["stats.last_30day_views", "ads_stats.video_view_rate", "ads_stats.ctr_v"]
    ALLOWED_CATEGORY_SORTS = ["stats.last_30day_subscribers", "stats.last_30day_views", "ads_stats.video_view_rate",
                             "ads_stats.ctr_v"]

    def get(self, request, *args, **kwargs):
        params = str(request.query_params) + str(kwargs)
        cache_key = get_cache_key(params, prefix=DASHBOARD_INDUSTRY_PERFORMANCE_CACHE_PREFIX)
        try:
            cache = CacheItem.objects.get(key=cache_key).value
            if cache.updated_at < now_in_default_tz() - timedelta(seconds=self.CACHE_TTL):
                cache.value = self._get_data(request)
                cache.save()
            data = cache.value
        except CacheItem.DoesNotExist:
            data = self._get_data(request)
            CacheItem.objects.create(key=cache_key, value=json.dumps(data))
        return Response(data=data)

    def _get_data(self, request):
        channel_sort = request.query_params.get("channel_sort") or "stats.last_30day_subscribers"
        video_sort = request.query_params.get("video_sort") or "stats.last_30day_views"
        category_sort = request.query_params.get("category_sort") or "stats.last_30day_subscribers"
        channel_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS),
                                         upsert_sections=())
        video_manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS),
                                     upsert_sections=())
        channel_forced_filters = channel_manager.forced_filters(include_deleted=False)
        video_forced_filters = video_manager.forced_filters(include_deleted=False)
        category_forced_filters = channel_manager.forced_filters(include_deleted=False)

        channel_sorting = {
            channel_sort: {
                "order": "desc"
            }
        }

        video_sorting = {
            video_sort: {
                "order": "desc"
            }
        }

        category_sorting = {
            category_sort: {
                "order": "desc"
            }
        }

        top_channels = channel_manager.search(filters=channel_forced_filters, sort=channel_sorting, limit=10)
        top_videos = video_manager.search(filters=video_forced_filters, sort=video_sorting, limit=10)

        pass
        # return data
