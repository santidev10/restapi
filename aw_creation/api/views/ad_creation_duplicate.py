from django.db.models import Q

from aw_creation.api.serializers import AdCreationSetupSerializer
from aw_creation.models import AdCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from userprofile.constants import StaticPermissions
from .base_creation_duplicate import BaseCreationDuplicateApiView


class AdCreationDuplicateApiView(BaseCreationDuplicateApiView):
    serializer_class = AdCreationSetupSerializer
    permission_classes = (StaticPermissions()(StaticPermissions.MEDIA_BUYING),)
    is_demo = Q(ad__ad_group__campaign__account_id=DEMO_ACCOUNT_ID)

    def get_queryset(self):
        queryset = AdCreation.objects.filter(
            Q(ad_group_creation__campaign_creation__account_creation__owner=self.request.user)
            | self.is_demo,
        )
        return queryset
