from rest_framework.generics import RetrieveAPIView

from performiq.api.serializers import IQCampaignSerializer
from performiq.models import IQCampaign
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission


class PerformIQCampaignRetrieveAPIView(RetrieveAPIView):
    queryset = IQCampaign.objects.all()
    serializer_class = IQCampaignSerializer
    permission_classes = (
        has_static_permission(StaticPermissions.PERFORMIQ),
    )
