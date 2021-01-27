from django.db.models import Q
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.serializers import AdCreationSetupSerializer
from aw_creation.api.serializers import AppendAdCreationSetupSerializer
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.views import forbidden_for_demo
from userprofile.constants import StaticPermissions


class AdCreationListSetupApiView(ListCreateAPIView):
    serializer_class = AdCreationSetupSerializer
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MEDIA_BUYING),)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = AdCreation.objects.filter(
            Q(ad_group_creation__campaign_creation__account_creation__owner=self.request.user)
            | Q(ad__ad_group__campaign__account_id=DEMO_ACCOUNT_ID)) \
            .filter(ad_group_creation_id=pk,
                    is_deleted=False,
                    )
        return queryset

    @forbidden_for_demo(lambda *args, **kwargs: AdGroupCreation.objects.filter(
        pk=kwargs.get("pk"),
        ad_group__campaign__account_id=DEMO_ACCOUNT_ID).exists()
                        )
    def create(self, request, *args, **kwargs):
        try:
            ad_group_creation = AdGroupCreation.objects.get(
                pk=kwargs.get("pk"),
                campaign_creation__account_creation__owner=request.user
            )
        except AdGroupCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        count = self.get_queryset().count()
        data = dict(
            name="Ad {}".format(count + 1),
            ad_group_creation=ad_group_creation.id,
        )
        serializer = AppendAdCreationSetupSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        obj = serializer.save()

        data = self.get_serializer(instance=obj).data
        return Response(data, status=HTTP_201_CREATED)
