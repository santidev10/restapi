from rest_framework.generics import ListAPIView

from aw_creation.api.serializers import AudienceHierarchySerializer
from aw_reporting.models import Audience
from userprofile.constants import StaticPermissions


class AudienceFlatListApiView(ListAPIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MANAGED_SERVICE),)
    serializer_class = AudienceHierarchySerializer

    def get_queryset(self):
        queryset = Audience.objects.all()
        if "title" in self.request.query_params:
            titles = self.request.query_params.getlist("title")
            queryset = queryset.filter(name__in=titles)
        return queryset
