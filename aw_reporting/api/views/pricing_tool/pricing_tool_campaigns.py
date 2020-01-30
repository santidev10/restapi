from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.tools.pricing_tool import PricingTool


class PricingToolCampaignsView(APIView):
    @staticmethod
    def post(request):
        campaigns_ids = request.data.get("campaigns", [])
        data = PricingTool.get_campaigns_data(campaigns_ids)

        return Response(data=data, status=HTTP_200_OK)
