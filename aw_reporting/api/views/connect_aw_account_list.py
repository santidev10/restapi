from drf_yasg.utils import swagger_auto_schema
from rest_framework.generics import ListAPIView

from aw_reporting.api.serializers import AWAccountConnectionRelationsSerializer
from aw_reporting.models import AWConnectionToUserRelation


class ConnectAWAccountListApiView(ListAPIView):
    serializer_class = AWAccountConnectionRelationsSerializer

    def get_queryset(self):
        qs = AWConnectionToUserRelation.objects.filter(
            user=self.request.user).order_by("connection__email")
        return qs

    @swagger_auto_schema(
        operation_description="List of AdWords accounts allowed to push data"
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

