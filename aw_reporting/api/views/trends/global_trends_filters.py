from rest_framework.response import Response

from aw_reporting.api.views.trends.base_track_filter_list import BaseTrackFiltersListApiView
from aw_reporting.tools.trends_tool.global_filters import GlobalTrendsFilters
from cache.constants import GLOBAL_TRENDS_FILTERS_KEY
from cache.models import CacheItem


class GlobalTrendsFiltersApiView(BaseTrackFiltersListApiView):
    filter_class = GlobalTrendsFilters

    def get(self, request, *args, **kwargs):
        user = request.user
        try:
            cached_filters_object = CacheItem.objects.get(key=f"{user.id}_{GLOBAL_TRENDS_FILTERS_KEY}")
            global_trends_filters = cached_filters_object.value
        except CacheItem.DoesNotExist:
            global_trends_filters = self.filter_class().get_filters(user)

            cached_filters_object, _ = CacheItem.objects.get_or_create(key=f"{user.id}_{GLOBAL_TRENDS_FILTERS_KEY}")
            cached_filters_object.value = global_trends_filters
            cached_filters_object.save()

        return Response(data=global_trends_filters)
