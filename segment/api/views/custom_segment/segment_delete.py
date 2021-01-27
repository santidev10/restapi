from rest_framework.exceptions import ValidationError
from rest_framework.generics import DestroyAPIView

from segment.api.serializers import CTLSerializer
from segment.models import CustomSegment
from segment.models.constants import SegmentActionEnum
from segment.models.utils.segment_action import segment_action
from segment.utils.utils import AdminCustomSegmentOwnerPermission
from userprofile.constants import StaticPermissions
from utils.permissions import or_permission_classes


class SegmentDeleteApiView(DestroyAPIView):
    serializer_class = CTLSerializer
    permission_classes = (
         or_permission_classes(
             AdminCustomSegmentOwnerPermission,
             StaticPermissions.has_perms(StaticPermissions.CTL__DELETE),
         ),
    )

    @segment_action(SegmentActionEnum.DELETE.value)
    def delete(self, request, *args, **kwargs):
        segment = self.get_object()
        if segment.audit_id:
            raise ValidationError("Vetted lists can not be deleted.")
        segment.s3.delete_export()
        return super().delete(request, *args, **kwargs)

    def get_queryset(self):
        queryset = CustomSegment.objects.filter(id=self.kwargs["pk"])
        return queryset
