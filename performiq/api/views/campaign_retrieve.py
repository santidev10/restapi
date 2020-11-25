from rest_framework.generics import RetrieveAPIView

from performiq.api.serializers import IQCampaignSerializer
from performiq.models import IQCampaign


class PerformIQCampaignRetrieveAPIView(RetrieveAPIView):
    queryset = IQCampaign.objects.all()
    serializer_class = IQCampaignSerializer
