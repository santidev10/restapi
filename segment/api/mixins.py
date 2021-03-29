import hashlib

from django.core.exceptions import PermissionDenied

from segment.models import CustomSegment
from segment.models import ParamsTemplate
from segment.utils.utils import get_persistent_segment_model_by_type
from segment.utils.utils import validate_segment_type
from userprofile.constants import StaticPermissions
from utils.utils import get_hash

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


class ParamsTemplateMixin:
    """
    Mixin for checking params template permission,
    creating new param templates,
    updating existing param templates,
    and implementing hashing method
    """

    @staticmethod
    def check_params_template_permissions(user, template_title):
        """
        ensure user has permission to update or create params template if template_title in request
        :param user: UserProfile
        :param template_title: str or None
        :return:
        """
        if template_title is None:
            return False
        if not user.has_permission(StaticPermissions.BUILD__CTL_PARAMS_TEMPLATE):
            raise PermissionDenied
        return True

    @staticmethod
    def create_update_params_template(user, template_title, params):
        """
        Creates new params template for user if title does not exist for segment type,
        otherwise updates with new params
        :user: userprofile.UserProfile type
        :template_title: str
        :params: dict
        :return:
        """
        title_hash = get_hash(template_title.lower().strip())
        segment_type = params.get("segment_type", None)
        if segment_type is not None and isinstance(segment_type, int):
            filter_args = dict((
                ("title_hash", title_hash),
                ("segment_type", segment_type),
            ))
            create_update_args = filter_args | dict((
                ("owner", user),
                ("title", template_title),
            ))
            object = ParamsTemplate.objects.filter(
                **filter_args
            ).get_or_create(**create_update_args)[0]
            object.params = params
            object.save()
            return
        raise TypeError("Valid segment type must be provided.")
