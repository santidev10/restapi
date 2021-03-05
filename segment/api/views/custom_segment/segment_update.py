from rest_framework.exceptions import PermissionDenied
from rest_framework.generics import UpdateAPIView

from segment.api.mixins import SegmentTypePermissionMixin
from segment.api.serializers.custom_segment_update_serializers import CustomSegmentAdminUpdateSerializer
from segment.api.serializers.custom_segment_update_serializers import CustomSegmentUpdateSerializer
from segment.models import CustomSegment
from segment.models.constants import SegmentTypeEnum
from segment.utils.utils import AdminCustomSegmentOwnerPermission
from userprofile.constants import StaticPermissions
from utils.permissions import or_permission_classes


class CustomSegmentUpdateApiView(UpdateAPIView, SegmentTypePermissionMixin):
    permission_classes = (
        or_permission_classes(
            AdminCustomSegmentOwnerPermission,
            StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST,
                                        StaticPermissions.BUILD__CTL_FEATURE_LIST),
            StaticPermissions.has_perms(StaticPermissions.BUILD__CTL_CREATE_VIDEO_LIST,
                                        StaticPermissions.BUILD__CTL_FEATURE_LIST)
        ),
    )
    permission_by_segment_type = {
        SegmentTypeEnum.VIDEO.value: StaticPermissions.BUILD__CTL_CREATE_VIDEO_LIST,
        SegmentTypeEnum.CHANNEL.value: StaticPermissions.BUILD__CTL_CREATE_CHANNEL_LIST
    }

    def get_object(self):
        pk = self.kwargs.get("pk", None)
        return CustomSegment.objects.get(pk=pk)

    def patch(self, request, *args, **kwargs):
        """
        override the default patch method to check segment type permissions.
        Checking with a permission class breaks file uploads on access to request.data
        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        segment = self.get_object()
        self.check_segment_type_permissions(request=request, segment_type=segment.segment_type)
        return super().patch(request, *args, **kwargs)

    def get_serializer(self, instance, *args, **kwargs):
        """
        Return the serializer instance that should be used for validating and
        deserializing input, and for serializing output.
        overwrites parent to pass instance to get_serializer_class so we can
        pass the correct serializer
        """
        serializer_class = self.get_serializer_class(instance)
        kwargs["context"] = self.get_serializer_context()
        return serializer_class(instance, *args, **kwargs)

    def get_serializer_class(self, instance):
        """
        return an update serializer based on user's permissions
        """
        if self.request.user.has_permission(StaticPermissions.BUILD__CTL_FEATURE_LIST):
            return CustomSegmentAdminUpdateSerializer
        if self.request.user.id != instance.owner_id:
            raise PermissionDenied("You do not have sufficient privileges to modify this resource.")
        return CustomSegmentUpdateSerializer
