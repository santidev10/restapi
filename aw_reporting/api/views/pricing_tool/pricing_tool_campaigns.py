from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from aw_reporting.tools.pricing_tool import PricingTool
from userprofile.constants import StaticPermissions


class PricingToolCampaignsView(APIView):
    permission_classes = (StaticPermissions()(StaticPermissions.PRICING_TOOL),)

    @staticmethod
    def post(request):
        campaigns_ids = request.data.get("campaigns", [])
        tool = PricingTool(user=request.user, **request.data)
        data = tool.get_campaigns_data(campaigns_ids)

        return Response(data=data, status=HTTP_200_OK)
