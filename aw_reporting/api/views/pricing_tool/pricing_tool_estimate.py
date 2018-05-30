from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from aw_reporting.tools.pricing_tool import PricingTool


class PricingToolEstimateView(APIView):
    @staticmethod
    def post(request):
        toll_obj = PricingTool(request.user, **request.data)
        return Response(data=toll_obj.estimate, status=HTTP_200_OK)
