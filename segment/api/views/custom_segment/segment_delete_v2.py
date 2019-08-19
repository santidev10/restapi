from rest_framework.generics import DestroyAPIView

from segment.api.serializers.serializers import CustomSegmentSerializer
from segment.custom_segment_export_generator import CustomSegmentExportGenerator
from segment.models import CustomSegment


class SegmentDeleteApiViewV2(DestroyAPIView):
    serializer_class = CustomSegmentSerializer

    def delete(self, request, *args, **kwargs):
        segment = self.get_object()
        CustomSegmentExportGenerator().delete_export(segment.owner.id, segment.title)
        return super().delete(request, *args, **kwargs)

    def get_queryset(self):
        queryset = CustomSegment.objects.filter(owner=self.request.user, id=self.kwargs["pk"])
        return queryset

