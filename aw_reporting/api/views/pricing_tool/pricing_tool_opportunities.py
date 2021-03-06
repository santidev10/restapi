from rest_framework.views import APIView

from aw_reporting.api.views.pagination import PricingToolCampaignsPagination
from aw_reporting.tools.pricing_tool import PricingTool
from userprofile.constants import StaticPermissions


class PricingToolOpportunityView(APIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.PRICING_TOOL),)

    @staticmethod
    def post(request):
        user = request.user
        toll_obj = PricingTool(user=user, **request.data)
        paginator = PricingToolCampaignsPagination()
        queryset = toll_obj.get_opportunities_queryset()
        page_opportunities = paginator.paginate_queryset(queryset, request)
        return paginator.get_paginated_response(data=toll_obj.get_opportunities_data(page_opportunities))
