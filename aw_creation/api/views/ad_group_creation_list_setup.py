from django.db.models import Q
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.serializers import AdGroupCreationSetupSerializer
from aw_creation.api.serializers import AppendAdGroupCreationSetupSerializer
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.views import forbidden_for_demo
from utils.permissions import MediaBuyingAddOnPermission


class AdGroupCreationListSetupApiView(ListCreateAPIView):
    serializer_class = AdGroupCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = AdGroupCreation.objects.filter(Q(campaign_creation__account_creation__owner=self.request.user)
                                                  | Q(ad_group__campaign__account_id=DEMO_ACCOUNT_ID)) \
            .filter(campaign_creation_id=pk,
                    is_deleted=False,
                    )
        return queryset

    @forbidden_for_demo(lambda *args, **kwargs: CampaignCreation.objects.filter(pk=kwargs.get("pk"),
                                                                                campaign__account_id=DEMO_ACCOUNT_ID).exists())
    def create(self, request, *args, **kwargs):
        try:
            campaign_creation = CampaignCreation.objects.filter(
                Q(account_creation__owner=request.user)
                | Q(campaign__account_id=DEMO_ACCOUNT_ID)
            ) \
                .get(pk=kwargs.get("pk"))
        except CampaignCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        count = self.get_queryset().count()
        data = dict(
            name="Ad Group {}".format(count + 1),
            campaign_creation=campaign_creation.id,
        )
        serializer = AppendAdGroupCreationSetupSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        ad_group_creation = serializer.save()

        AdCreation.objects.create(
            name="Ad 1",
            ad_group_creation=ad_group_creation,
        )
        data = self.get_serializer(instance=ad_group_creation).data
        return Response(data, status=HTTP_201_CREATED)
