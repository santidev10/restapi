from django.core.exceptions import PermissionDenied

from segment.models import CustomSegment
from segment.utils.utils import get_persistent_segment_model_by_type
from segment.utils.utils import validate_segment_type


class DynamicPersistentModelViewMixin:
    def dispatch(self, request, segment_type, **kwargs):
        self.model = get_persistent_segment_model_by_type(segment_type)
        if hasattr(self, "serializer_class"):
            self.serializer_class.Meta.model = self.model
        return super().dispatch(request, **kwargs)

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        queryset = self.model.objects.all().order_by("title")
        return queryset


class SegmentTypePermissionMixin:
    """
    mixin class for Views to check segment type permissions. Can't check segment type in a Permission class,
    since accessing request.data breaks fileuploads. Parsing from request.body requires a ton of fault tolerance
    """
    def check_segment_type_permissions(self, request, segment_type: int, allow_if_owner: bool = False,
                                       segment: CustomSegment = None):
        """
        checks permission based on segment type.
        NOTE: if a segment instance is passed, automatically allow if the requesting user is the owner
        :param request:
        :param segment_type:
        :param segment:
        :return:
        """
        if allow_if_owner and not isinstance(segment, CustomSegment):
            raise ValueError("a CustomSegment instance must be supplied when `allow_if_owner` is True!")

        if allow_if_owner and isinstance(segment, CustomSegment) and segment.owner == request.user:
            return True
        validate_segment_type(segment_type)
        required_permission = self.permission_by_segment_type.get(segment_type)
        if required_permission is None or not request.user.has_permission(required_permission):
            raise PermissionDenied
