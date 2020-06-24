from rest_framework.generics import ListAPIView

from aw_creation.api.serializers import AudienceHierarchySerializer
from aw_reporting.models import Audience


class AudienceFlatListApiView(ListAPIView):
    serializer_class = AudienceHierarchySerializer

    def get_queryset(self):
        queryset = Audience.objects.all()
        if "title" in self.request.query_params:
            titles = self.request.query_params.getlist("title")
            queryset = queryset.filter(name__in=titles)
        return queryset
