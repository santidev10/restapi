from rest_framework.generics import ListAPIView

from aw_creation.api.serializers import TopicHierarchySerializer
from aw_reporting.models import Topic
from userprofile.constants import StaticPermissions


class TopicToolFlatListApiView(ListAPIView):
    permission_classes = (StaticPermissions()(StaticPermissions.MEDIA_BUYING),)
    serializer_class = TopicHierarchySerializer

    def get_queryset(self):
        queryset = Topic.objects.all()
        if "title" in self.request.query_params:
            titles = self.request.query_params.getlist("title")
            queryset = queryset.filter(
                name__in=titles)
        return queryset
