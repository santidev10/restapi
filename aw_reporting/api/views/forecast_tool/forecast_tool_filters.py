from rest_framework.generics import RetrieveAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.tools.forecast_tool.forecast_tool import ForecastTool
from cache.constants import FORECAST_TOOL_FILTERS_KEY
from cache.models import CacheItem
from userprofile.constants import StaticPermissions


class ForecastToolFiltersApiView(RetrieveAPIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.FORECAST_TOOL),
    )

    def get(self, request, *args, **kwargs):
        try:
            cached_filters_object = CacheItem.objects.get(key=FORECAST_TOOL_FILTERS_KEY)
            forecast_tool_filters = cached_filters_object.value
        except CacheItem.DoesNotExist:
            forecast_tool_filters = ForecastTool.get_filters()

            cached_filters_object, _ = CacheItem.objects.get_or_create(key=FORECAST_TOOL_FILTERS_KEY)
            cached_filters_object.value = forecast_tool_filters
            cached_filters_object.save()

        return Response(data=forecast_tool_filters, status=HTTP_200_OK)
