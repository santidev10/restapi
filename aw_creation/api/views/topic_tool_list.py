from rest_framework.generics import ListAPIView

from aw_creation.api.serializers import TopicHierarchySerializer
from aw_reporting.models import Topic


class TopicToolListApiView(ListAPIView):
    serializer_class = TopicHierarchySerializer

    def get_queryset(self):
        queryset = Topic.objects.filter(parent__isnull=True).order_by("name")
        if "ids" in self.request.query_params:
            queryset = Topic.objects.all()
            queryset = queryset.filter(
                id__in=self.request.query_params["ids"].split(","))
        return queryset
