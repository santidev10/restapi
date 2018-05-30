from rest_framework.views import APIView

from aw_reporting.api.views.pagination import PricingToolCampaignsPagination
from aw_reporting.tools.pricing_tool import PricingTool


class PricingToolOpportunityView(APIView):
    @staticmethod
    def post(request):
        toll_obj = PricingTool(request.user, **request.data)
        paginator = PricingToolCampaignsPagination()
        queryset = toll_obj.get_opportunities_queryset()
        page_campaigns = paginator.paginate_queryset(
            queryset, request
        )
        return paginator.get_paginated_response(
            data=toll_obj.get_opportunities_data(page_campaigns)
        )
