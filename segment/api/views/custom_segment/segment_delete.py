from rest_framework.exceptions import ValidationError
from rest_framework.generics import DestroyAPIView

from segment.api.serializers import CTLSerializer
from segment.models import CustomSegment
from segment.models.constants import SegmentActionEnum
from segment.models.constants import SegmentTypeEnum
from segment.models.utils.segment_action import segment_action
from segment.utils.utils import AdminCustomSegmentOwnerPermission
from segment.utils.utils import CustomSegmentChannelDeletePermission
from segment.utils.utils import CustomSegmentVideoDeletePermission
from utils.permissions import or_permission_classes

from segment.api.mixins import SegmentTypePermissionMixin
from userprofile.constants import StaticPermissions


class SegmentDeleteApiView(DestroyAPIView, SegmentTypePermissionMixin):
    serializer_class = CTLSerializer
    permission_classes = (
         or_permission_classes(
             AdminCustomSegmentOwnerPermission,
             StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_DELETE_CHANNEL_LIST),
             StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_DELETE_VIDEO_LIST),
         ),
    )
    permission_by_segment_type = {
        SegmentTypeEnum.VIDEO.value: StaticPermissions.BUILD__CTL_DELETE_VIDEO_LIST,
        SegmentTypeEnum.CHANNEL.value: StaticPermissions.BUILD__CTL_DELETE_CHANNEL_LIST
    }

    @segment_action(SegmentActionEnum.DELETE.value)
    def delete(self, request, *args, **kwargs):
        segment = self.get_object()
        self.check_segment_type_permissions(request=request, segment_type=segment.segment_type)
        if segment.audit_id:
            raise ValidationError("Vetted lists can not be deleted.")
        segment.s3.delete_export()
        return super().delete(request, *args, **kwargs)

    def get_queryset(self):
        queryset = CustomSegment.objects.filter(id=self.kwargs["pk"])
        return queryset
