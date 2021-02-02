from django.db.models import Q

from aw_creation.api.serializers import AdGroupCreationSetupSerializer
from aw_creation.models import AdGroupCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from userprofile.constants import StaticPermissions
from .base_creation_duplicate import BaseCreationDuplicateApiView


class AdGroupCreationDuplicateApiView(BaseCreationDuplicateApiView):
    serializer_class = AdGroupCreationSetupSerializer
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MEDIA_BUYING),)
    is_demo = Q(ad_group__campaign__account_id=DEMO_ACCOUNT_ID)

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            Q(campaign_creation__account_creation__owner=self.request.user)
            | self.is_demo,
        )
        return queryset
