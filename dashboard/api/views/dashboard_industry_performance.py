from datetime import timedelta
import json

from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

from cache.models import CacheItem
from dashboard.api.views.constants import DASHBOARD_INDUSTRY_PERFORMANCE_CACHE_PREFIX
from dashboard.utils import get_cache_key
from es_components.constants import Sections
from es_components.iab_categories import TOP_LEVEL_CATEGORIES
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from utils.datetime import now_in_default_tz
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class DashboardIndustryPerformanceAPIView(APIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.channel_list"),
            IsAdminUser
        ),
    )

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

    def get_category_widget_aggregations(self, manager, categories, size=0):
        search = manager._search()
        aggregation = {}
        for category in categories:
            aggregation[f"{category}"] = {
                "filter": {
                    "bool": {
                        "must": {
                            "term": {
                                "general_data.iab_categories": category
                            }
                        }
                    }
                },
                "aggs": {
                    "stats.last_30day_subscribers": {
                        "sum": {
                            "field": "stats.last_30day_subscribers"
                        }
                    },
                    "stats.last_30day_views": {
                        "sum": {
                            "field": "stats.last_30day_views"
                        }
                    },
                    "ads_stats.video_view_rates": {
                        "avg": {
                            "field": "ads_stats.video_view_rate"
                        }
                    },
                    "ads_stats.ctr_v": {
                        "avg": {
                            "field": "ads_stats.ctr_v"
                        }
                    }
                }
            }
        search.update_from_dict({
            "size": size,
            "aggs": aggregation
        })
        aggregations_result = search.execute().aggregations.to_dict()
        return aggregations_result

    def _get_data(self, request):
        channel_sort = request.query_params.get("channel_sort") \
            if request.query_params.get("channel_sort") in self.ALLOWED_CHANNEL_SORTS \
            else "stats.last_30day_subscribers"
        video_sort = request.query_params.get("video_sort") \
            if request.query_params.get("video_sort") in self.ALLOWED_VIDEO_SORTS else "stats.last_30day_views"
        category_sort = request.query_params.get("category_sort") \
            if request.query_params.get("category_sort") in self.ALLOWED_CATEGORY_SORTS \
            else "stats.last_30day_subscribers"
        channel_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS),
                                         upsert_sections=())
        video_manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS),
                                     upsert_sections=())
        channel_forced_filters = channel_manager.forced_filters(include_deleted=False)
        video_forced_filters = video_manager.forced_filters(include_deleted=False)

        channel_sorting = [
            {
                channel_sort: {
                    "order": "desc"
                }
            }
        ]
        video_sorting = [
            {
                video_sort: {
                    "order": "desc"
                }
            }
        ]

        top_channels = channel_manager.search(filters=channel_forced_filters, sort=channel_sorting,
                                              limit=10).execute().hits
        top_videos = video_manager.search(filters=video_forced_filters, sort=video_sorting, limit=10).execute().hits

        t1_categories = [category.title() for category in TOP_LEVEL_CATEGORIES]
        category_aggregations = self.get_category_widget_aggregations(manager=channel_manager, categories=t1_categories)
        top_categories = []
        for key, value in category_aggregations.items():
            value["key"] = key
            top_categories.append(value)
        top_categories = sorted(top_categories, key=lambda category: -category[category_sort]["value"])[:10]

        data = {
            "top_channels": top_channels,
            "top_videos": top_videos,
            "top_categories": top_categories
        }
        return data
