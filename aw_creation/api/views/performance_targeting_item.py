from django.shortcuts import get_object_or_404
from rest_framework.generics import UpdateAPIView

from aw_creation.api.serializers import UpdateTargetingDirectionSerializer
from aw_creation.models import AdGroupCreation
from aw_creation.models import TargetingItem
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.views import forbidden_for_demo
from aw_reporting.models import AdGroup


class PerformanceTargetingItemAPIView(UpdateAPIView):
    serializer_class = UpdateTargetingDirectionSerializer

    @forbidden_for_demo(lambda *args, **kwargs: AdGroup.objects.filter(pk=kwargs["ad_group_id"],
                                                                       campaign__account_id=DEMO_ACCOUNT_ID).exists())
    def update(self, request, *args, **kwargs):
        return super(PerformanceTargetingItemAPIView, self).update(request, *args, **kwargs)

    def get_object(self):
        targeting_type = self.kwargs["targeting"].lower()
        if targeting_type.endswith("s"):
            targeting_type = targeting_type[:-1]

        ad_group_creation = get_object_or_404(
            AdGroupCreation,
            campaign_creation__account_creation__owner=self.request.user,
            ad_group_id=self.kwargs["ad_group_id"],
        )
        obj = get_object_or_404(
            TargetingItem, criteria=self.kwargs["criteria"],
            ad_group_creation=ad_group_creation, type=targeting_type,
        )
        return obj
