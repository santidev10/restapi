from rest_framework.views import APIView
from rest_framework.response import Response

from performiq.models import OAuthAccount
from performiq.models import Campaign
from performiq.models.constants import OAuthType
from performiq.api.serializers import GoogleAdsCampaignSerializer


class PerfromIQCampaignsAPIView(APIView):
    def get(self, request, *args, **kwargs):
        oauth_account = OAuthAccount.objects.get(user=request.user)
        gads_campaign_qs = Campaign.objects.filter(account=oauth_account, oauth_type=OAuthType.GOOGLE_ADS.value)
        response = {
            "google_ads": {
                "email": oauth_account.email,
                "campaigns": GoogleAdsCampaignSerializer(gads_campaign_qs, many=True).data
            },
            "dv360": [],
        }
        return Response(data=response)