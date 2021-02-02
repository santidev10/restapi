from rest_framework.generics import RetrieveAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.tools.pricing_tool import PricingTool
from cache.constants import PRICING_TOOL_FILTERS_KEY
from cache.models import CacheItem
from userprofile.constants import StaticPermissions


class PricingToolFiltersView(RetrieveAPIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.PRICING_TOOL),)

    def get(self, request, *args, **kwargs):
        user = request.user
        try:
            cached_filters_object = CacheItem.objects.get(key=f"{user.id}_{PRICING_TOOL_FILTERS_KEY}")
            pricing_tool_filters = cached_filters_object.value
        except CacheItem.DoesNotExist:
            pricing_tool_filters = PricingTool.get_filters(user=user)

            cached_filters_object, _ = CacheItem.objects.get_or_create(key=f"{user.id}_{PRICING_TOOL_FILTERS_KEY}")
            cached_filters_object.value = pricing_tool_filters
            cached_filters_object.save()

        return Response(data=pricing_tool_filters, status=HTTP_200_OK)
