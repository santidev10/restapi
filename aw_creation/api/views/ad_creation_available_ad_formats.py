from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AdCreation
from userprofile.constants import StaticPermissions


class AdCreationAvailableAdFormatsApiView(APIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MEDIA_BUYING),)

    @swagger_auto_schema(
        operation_description="Get Ad group creation",
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
    def get(self, request, pk, **_):
        try:
            ad_creation = AdCreation.objects.get(pk=pk)
        except AdCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        return Response(
            ad_creation.ad_group_creation.get_available_ad_formats())
