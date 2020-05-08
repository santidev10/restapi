from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import update_or_create_campaign_creation
from aw_creation.api.views.media_buying.utils import update_or_create_ad_group_creation


class AccountAdGroupAPIView(APIView):
    def patch(self, request, *args, **kwargs):
        account_creation_id = kwargs["account_id"]
        campaign_id = kwargs["campaign_id"]
        ad_group_id = kwargs["ad_group_id"]
        data = request.data
        account_creation = get_account_creation(request.user, account_creation_id)
        campaign = Campaign.objects.get(id=campaign_id)
        ad_group = AdGroup.objects.get(id=ad_group_id)
        data = {
            "status": data["status"],
        }
        campaign_creation = update_or_create_campaign_creation(account_creation, campaign)
        ad_group_creation = update_or_create_ad_group_creation(campaign_creation, ad_group, params=data)
        return Response(ad_group_creation.id)
