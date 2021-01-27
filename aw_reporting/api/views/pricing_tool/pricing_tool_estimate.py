from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from aw_reporting.tools.pricing_tool import PricingTool
from userprofile.constants import StaticPermissions


class PricingToolEstimateView(APIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.PRICING_TOOL),)

    @staticmethod
    def post(request):
        toll_obj = PricingTool(user=request.user, **request.data)
        return Response(data=toll_obj.estimate, status=HTTP_200_OK)
