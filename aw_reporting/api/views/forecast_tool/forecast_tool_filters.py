from rest_framework.generics import RetrieveAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.tools.forecast_tool.forecast_tool import ForecastTool


class ForecastToolFiltersApiView(RetrieveAPIView):
    def get(self, request, *args, **kwargs):
        response = ForecastTool.get_filters()
        return Response(data=response, status=HTTP_200_OK)
