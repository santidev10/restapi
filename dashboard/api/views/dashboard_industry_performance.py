from datetime import timedelta
import json

from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

from cache.models import CacheItem
from dashboard.api.views.constants import DASHBOARD_INDUSTRY_PERFORMANCE_CACHE_PREFIX
from dashboard.utils import get_cache_key
from es_components.constants import Sections
from es_components.iab_categories import IAB_TIER1_CATEGORIES
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from utils.datetime import now_in_default_tz
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class DashboardIndustryPerformanceAPIView(APIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.channel_list"),
            user_has_permission("userprofile.settings_my_yt_channels"),
            IsAdminUser
        ),
    )

    CACHE_TTL = 3600
    ALLOWED_CHANNEL_SORTS = ["stats.last_30day_subscribers", "stats.last_30day_views", "ads_stats.video_view_rate",
                             "ads_stats.ctr_v"]
    ALLOWED_VIDEO_SORTS = ["stats.last_30day_views", "ads_stats.video_view_rate", "ads_stats.ctr_v"]
    ALLOWED_CATEGORY_SORTS = ["stats.last_30day_subscribers", "stats.last_30day_views", "ads_stats.video_view_rate",
                              "ads_stats.ctr_v"]
    TOP_HITS_COUNT = 10

    def get(self, request, *args, **kwargs):
        params = request.query_params

        # Get Sorting Parameters
        channel_sort = params.get("channel_sort") \
            if params.get("channel_sort") in self.ALLOWED_CHANNEL_SORTS \
            else "stats.last_30day_subscribers"
        video_sort = params.get("video_sort") \
            if params.get("video_sort") in self.ALLOWED_VIDEO_SORTS else "stats.last_30day_views"
        category_sort = params.get("category_sort") \
            if params.get("category_sort") in self.ALLOWED_CATEGORY_SORTS \
            else "stats.last_30day_subscribers"

        # Generate Cache Keys for Each Sorting Parameter
        channels_cache_key = get_cache_key(channel_sort,
                                           prefix=f"{DASHBOARD_INDUSTRY_PERFORMANCE_CACHE_PREFIX}channels_")
        videos_cache_key = get_cache_key(video_sort,
                                         prefix=f"{DASHBOARD_INDUSTRY_PERFORMANCE_CACHE_PREFIX}videos_")
        categories_cache_key = get_cache_key(category_sort,
                                             prefix=f"{DASHBOARD_INDUSTRY_PERFORMANCE_CACHE_PREFIX}categories_")

        # Get Channels Data
        channels_data = self.retrieve_from_cache(channels_cache_key)
        if channels_data is None:
            channel_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS),
                                             upsert_sections=())
            channel_forced_filters = channel_manager.forced_filters(include_deleted=False)
            channel_sorting = [
                {
                    channel_sort: {
                        "order": "desc"
                    }
                }
            ]
            channel_hits = channel_manager.search(filters=channel_forced_filters, sort=channel_sorting,
                                                  limit=self.TOP_HITS_COUNT).execute().hits
            top_channels = []
            for hit in channel_hits:
                channel = {
                    "id": hit.main.id,
                    "title": hit.general_data.title,
                    "stats.last_30day_subscribers": hit.stats.last_30day_subscribers,
                    "stats.last_30day_views": hit.stats.last_30day_views,
                    "ads_stats.video_view_rate": hit.ads_stats.video_view_rate,
                    "ads_stats.ctr_v": hit.ads_stats.ctr_v
                }
                top_channels.append(channel)
            channels_cache, _ = CacheItem.objects.get_or_create(key=channels_cache_key)
            channels_cache.value = json.dumps({"top_channels": list(top_channels)})
            channels_cache.save()
        else:
            top_channels = channels_data["top_channels"]

        # Get Videos Data
        videos_data = self.retrieve_from_cache(videos_cache_key)
        if videos_data is None:
            video_manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS),
                                         upsert_sections=())
            video_forced_filters = video_manager.forced_filters(include_deleted=False)
            video_sorting = [
                {
                    video_sort: {
                        "order": "desc"
                    }
                }
            ]
            video_hits = video_manager.search(filters=video_forced_filters, sort=video_sorting,
                                              limit=self.TOP_HITS_COUNT).execute().hits
            top_videos = []
            for hit in video_hits:
                video = {
                    "id": hit.main.id,
                    "title": hit.general_data.title,
                    "stats.last_30day_views": hit.stats.last_30day_views,
                    "ads_stats.video_view_rate": hit.ads_stats.video_view_rate,
                    "ads_stats.ctr_v": hit.ads_stats.ctr_v
                }
                top_videos.append(video)
            videos_cache, _ = CacheItem.objects.get_or_create(key=videos_cache_key)
            videos_cache.value = json.dumps({"top_videos": list(top_videos)})
            videos_cache.save()
        else:
            top_videos = videos_data["top_videos"]

        # Get Categories Data
        categories_data = self.retrieve_from_cache(categories_cache_key)
        if categories_data is None:
            category_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS),
                                              upsert_sections=())
            t1_categories = [category.title() for category in IAB_TIER1_CATEGORIES]
            category_aggregations = self.get_category_widget_aggregations(manager=category_manager,
                                                                          categories=t1_categories)
            top_categories = []
            for key, value in category_aggregations.items():
                value["key"] = key
                top_categories.append(value)
            top_categories = sorted(top_categories,
                                    key=lambda category: category[category_sort].get("value") or 0,
                                    reverse=True)[:self.TOP_HITS_COUNT]
            categories_cache, _ = CacheItem.objects.get_or_create(key=categories_cache_key)
            categories_cache.value = json.dumps({"top_categories": list(top_categories)})
            categories_cache.save()
        else:
            top_categories = categories_data["top_categories"]

        # Generate and return Response
        data = {
            "top_channels": list(top_channels),
            "top_videos": list(top_videos),
            "top_categories": top_categories
        }
        return Response(data=data)

    def retrieve_from_cache(self, cache_key):
        try:
            cache = CacheItem.objects.get(key=cache_key)
            if cache.updated_at < now_in_default_tz() - timedelta(seconds=self.CACHE_TTL):
                return None
            data = json.loads(cache.value)
            return data
        except CacheItem.DoesNotExist:
            return None

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
                    "ads_stats.video_view_rate": {
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
