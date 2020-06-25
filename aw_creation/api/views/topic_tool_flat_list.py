from rest_framework.generics import ListAPIView

from aw_creation.api.serializers import TopicHierarchySerializer
from aw_reporting.models import Topic


class TopicToolFlatListApiView(ListAPIView):
    serializer_class = TopicHierarchySerializer

    def get_queryset(self):
        queryset = Topic.objects.all()
        if "title" in self.request.query_params:
            titles = self.request.query_params.getlist("title")
            queryset = queryset.filter(
                name__in=titles)
        return queryset
