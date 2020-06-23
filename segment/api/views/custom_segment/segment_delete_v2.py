from rest_framework.exceptions import ValidationError
from rest_framework.generics import DestroyAPIView
from rest_framework.permissions import IsAdminUser

from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.models import CustomSegment
from utils.permissions import IsOwnerPermission
from utils.permissions import or_permission_classes


class SegmentDeleteApiViewV2(DestroyAPIView):
    serializer_class = CustomSegmentSerializer

    permission_classes = (
        or_permission_classes(
            IsAdminUser,
            IsOwnerPermission
        ),
    )

    def delete(self, request, *args, **kwargs):
        segment = self.get_object()
        if segment.audit_id:
            raise ValidationError("Vetted lists can not be deleted.")
        segment.delete_export()
        return super().delete(request, *args, **kwargs)

    def get_queryset(self):
        queryset = CustomSegment.objects.filter(owner=self.request.user, id=self.kwargs["pk"])
        return queryset
