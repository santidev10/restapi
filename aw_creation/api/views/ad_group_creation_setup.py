from django.db.models import Q
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.generics import RetrieveUpdateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_creation.api.serializers import AdGroupCreationSetupSerializer
from aw_creation.api.serializers import AdGroupCreationUpdateSerializer
from aw_creation.models import AdGroupCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.views import forbidden_for_demo
from userprofile.constants import StaticPermissions


class AdGroupCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = AdGroupCreationSetupSerializer
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MEDIA_BUYING),)

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                name="id",
                required=True,
                in_=openapi.IN_PATH,
                description="Ad Group creation id",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    def put(self, request, *args, **kwargs):
        return super().put(request, *args, **kwargs)

    def get_queryset(self):
        queryset = AdGroupCreation.objects.filter(
            Q(campaign_creation__account_creation__owner=self.request.user)
            | Q(ad_group__campaign__account_id=DEMO_ACCOUNT_ID)) \
            .filter(is_deleted=False)
        return queryset

    @forbidden_for_demo(lambda *args, **kwargs: AdGroupCreation.objects.filter(
        ad_group__campaign__account_id=DEMO_ACCOUNT_ID,
        pk=kwargs.get("pk")).exists())
    def update(self, request, *args, **kwargs):
        partial = kwargs.pop("partial", False)
        instance = self.get_object()
        serializer = AdGroupCreationUpdateSerializer(
            instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return self.retrieve(self, request, *args, **kwargs)

    def delete(self, *args, **_):
        instance = self.get_object()
        count = self.get_queryset().filter(
            campaign_creation=instance.campaign_creation).count()
        if count < 2:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="You cannot delete the only item"))
        instance.is_deleted = True
        instance.save()
        return Response(status=HTTP_204_NO_CONTENT)
