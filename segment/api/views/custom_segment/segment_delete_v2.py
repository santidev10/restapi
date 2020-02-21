from rest_framework.exceptions import ValidationError
from rest_framework.generics import DestroyAPIView

from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.models import CustomSegment


class SegmentDeleteApiViewV2(DestroyAPIView):
    serializer_class = CustomSegmentSerializer

    def delete(self, request, *args, **kwargs):
        segment = self.get_object()
        if segment.audit_id:
            raise ValidationError("Vetted lists can not be deleted.")
        segment.delete_export()
        return super().delete(request, *args, **kwargs)

    def get_queryset(self):
        queryset = CustomSegment.objects.filter(owner=self.request.user, id=self.kwargs["pk"])
        return queryset
