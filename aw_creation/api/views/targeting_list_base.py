from django.db.models import Q
from rest_framework.generics import GenericAPIView

from aw_creation.api.serializers import AdGroupTargetingListSerializer
from aw_creation.api.serializers import add_targeting_list_items_info
from aw_creation.models import TargetingItem
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from userprofile.constants import StaticPermissions


class TargetingListBaseAPIClass(GenericAPIView):
    permission_classes = (StaticPermissions()(StaticPermissions.MEDIA_BUYING),)
    serializer_class = AdGroupTargetingListSerializer

    def get_user(self):
        return self.request.user

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        list_type = self.kwargs.get("list_type")
        queryset = TargetingItem.objects.filter(
            Q(ad_group_creation__campaign_creation__account_creation__owner=self.get_user())
            | Q(ad_group_creation__ad_group__campaign__account_id=DEMO_ACCOUNT_ID)) \
            .filter(ad_group_creation_id=pk,
                    type=list_type,)
        return queryset

    @staticmethod
    def data_to_list(data):
        data = [i["criteria"] if isinstance(i, dict) else i for i in data]
        return data

    def data_to_dicts(self, data):
        is_negative = self.request.GET.get("is_negative", False)
        data = [i if isinstance(i, dict)
                else dict(criteria=str(i), is_negative=is_negative)
                for i in data]
        return data

    def add_items_info(self, data):
        list_type = self.kwargs.get("list_type")
        add_targeting_list_items_info(data, list_type)
