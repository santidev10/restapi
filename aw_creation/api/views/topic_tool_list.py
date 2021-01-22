from rest_framework.generics import ListAPIView

from aw_creation.api.serializers import TopicHierarchySerializer
from aw_reporting.models import Topic
from userprofile.constants import StaticPermissions


class TopicToolListApiView(ListAPIView):
    permission_classes = (StaticPermissions()(StaticPermissions.MEDIA_BUYING),)
    serializer_class = TopicHierarchySerializer

    def get_queryset(self):
        queryset = Topic.objects.filter(parent__isnull=True).order_by("name")
        if "ids" in self.request.query_params:
            queryset = Topic.objects.all()
            queryset = queryset.filter(
                id__in=self.request.query_params["ids"].split(","))
        return queryset
