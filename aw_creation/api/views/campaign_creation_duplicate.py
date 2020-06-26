from django.db.models import Q

from aw_creation.api.serializers import CampaignCreationSetupSerializer
from aw_creation.models import CampaignCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from utils.permissions import MediaBuyingAddOnPermission
from .base_creation_duplicate import BaseCreationDuplicateApiView


class CampaignCreationDuplicateApiView(BaseCreationDuplicateApiView):
    serializer_class = CampaignCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)
    is_demo = Q(campaign__account_id=DEMO_ACCOUNT_ID)

    def get_queryset(self):
        queryset = CampaignCreation.objects.filter(
            Q(account_creation__owner=self.request.user)
            | self.is_demo,
        )
        return queryset
