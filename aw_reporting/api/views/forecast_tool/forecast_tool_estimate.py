from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from aw_reporting.tools.forecast_tool.forecast_tool import ForecastTool


class ForecastToolEstimateApiView(APIView):
    def post(self, request):
        toll_obj = ForecastTool(**request.data)
        return Response(data=toll_obj.estimate, status=HTTP_200_OK)
