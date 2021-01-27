from rest_framework.generics import RetrieveAPIView

from performiq.api.serializers import IQCampaignSerializer
from performiq.models import IQCampaign
from userprofile.constants import StaticPermissions


class PerformIQCampaignRetrieveAPIView(RetrieveAPIView):
    queryset = IQCampaign.objects.all()
    serializer_class = IQCampaignSerializer
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.PERFORMIQ),
    )
