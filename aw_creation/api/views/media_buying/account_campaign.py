from rest_framework.views import APIView
from rest_framework.response import Response

from aw_reporting.models import Campaign
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import update_or_create_campaign_creation


class AccountCampaignAPIView(APIView):
    def patch(self, request, *args, **kwargs):
        account_creation_id = kwargs["account_id"]
        campaign_id = kwargs["campaign_id"]
        data = request.data
        account_creation = get_account_creation(request.user, account_creation_id)
        campaign = Campaign.objects.get(id=campaign_id)
        data = {
            "budget": data["budget"]
        }
        campaign_creation = update_or_create_campaign_creation(account_creation, campaign, data)
        return Response(campaign_creation.id)
