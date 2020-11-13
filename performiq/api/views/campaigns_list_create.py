from rest_framework.views import APIView
from rest_framework.response import Response
from distutils.util import strtobool

from .utils.get_campaigns import get_campaigns
from performiq.api.serializers import IQCampaignSerializer
from performiq.models import IQCampaign


class PerformIQCampaignListCreateAPIView(APIView):
    def get(self, request, *args, **kwargs):
        if strtobool(request.query_params.get("analyzed")):
            qs = IQCampaign.objects.filter(campaign__account__oauth_account__user=request.user)\
                 | IQCampaign.objects.filter(campaign__advertiser__oauth_accounts__user=request.user)
            data = IQCampaignSerializer(qs, many=True).data
        else:
            data = get_campaigns(request.user)
        return Response(data=data)

    def post(self, request, *args, **kwargs):
        serializer = IQCampaignSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_params = serializer.validated_data
        iq_campaign = serializer.save()
        return Response(validated_params)
