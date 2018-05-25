from rest_framework.generics import RetrieveAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.tools.pricing_tool import PricingTool
from utils.cache import cached_view_decorator as cached_view


class PricingToolFiltersView(RetrieveAPIView):

    @cached_view
    def get(self, request, *args, **kwargs):
        response = PricingTool.get_filters(request.user)

        return Response(data=response, status=HTTP_200_OK)
