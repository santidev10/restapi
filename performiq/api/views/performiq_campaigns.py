from rest_framework.views import APIView
from rest_framework.response import Response

from performiq.models import OAuthAccount
from performiq.models import Campaign
from performiq.models.constants import OAuthType
from performiq.api.serializers import GoogleAdsCampaignSerializer


class PerfromIQCampaignsAPIView(APIView):
    def get(self, request, *args, **kwargs):
        response = {
            "google_ads": {},
            "dv360": {},
        }
        gads_account = self._get_account(request.user, OAuthType.GOOGLE_ADS.value)
        dv360_account = self._get_account(request.user, OAuthType.DV360.value)
        if gads_account:
            gads_data = self._get_res_data(gads_account, Campaign, GoogleAdsCampaignSerializer, OAuthType.GOOGLE_ADS.value)
            response["google_ads"].update(gads_data)
        if dv360_account:
            dv360_data = {}
            response["dv360"].update(dv360_data)
        return Response(data=response)

    def _get_account(self, user, oauth_type):
        try:
            account = OAuthAccount.objects.get(user=user, oauth_type=oauth_type)
        except OAuthAccount.DoesNotExist:
            account = None
        return account

    def _get_res_data(self, account, campaign_model, campaign_serializer, oauth_type):
        campaign_qs = campaign_model.objects.filter(account=account, oauth_type=oauth_type)
        campaigns = campaign_serializer(campaign_qs, many=True).data
        data = {
            "email": account.email,
            "campaigns": campaigns,
        }
        return data
