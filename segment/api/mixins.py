from django.core.exceptions import PermissionDenied
from django.core.exceptions import ValidationError

from segment.api.serializers import ParamsTemplateSerializer
from segment.models.constants import SegmentTypeEnum
from segment.models import CustomSegment
from segment.models import ParamsTemplate
from segment.utils.utils import get_persistent_segment_model_by_type
from segment.utils.utils import validate_segment_type
from userprofile.constants import StaticPermissions
from utils.utils import get_hash
from utils.views import get_object


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
    Mixin for parameter templates
    """

    @staticmethod
    def check_params_template_permissions(user):
        """
        :param user:
        :return:
        """
        if not user.has_permission(StaticPermissions.BUILD__CTL_PARAMS_TEMPLATE):
            raise PermissionDenied

    @staticmethod
    def _update_params_template(user, template_id, params):
        """
        updates ParamsTemplate instance for a given id and new params
        :user: userprofile.UserProfile type
        :template_id: int
        :params: dict
        :return:
        """
        params_template = get_object(ParamsTemplate, id=template_id)
        if user.id == params_template.owner.id:
            params_template.params = params
            params_template.save()
            return params_template
        raise PermissionDenied("Cannot update a template owned by another user.")

    @staticmethod
    def _create_params_template(user, template_title, params):
        """
        Creates new ParamsTemplate instance
        :user:
        :template_title: str
        :params: dict
        :return:
        """
        title_hash = get_hash(template_title.lower().strip())
        segment_type = params.get("segment_type", None)
        if segment_type is not None and segment_type in \
                (SegmentTypeEnum.VIDEO.value, SegmentTypeEnum.CHANNEL.value):
            params_template = ParamsTemplate.objects.create(
                owner=user,
                segment_type=segment_type,
                title=template_title,
                title_hash=title_hash,
                params=params
            )
            params_template.save()
            return params_template
        raise ValidationError("Must provide a valid segment type.")

    @staticmethod
    def _get_templates_by_owner(user, segment_type):
        """
        returns serialized data of ParamsTemplate objects owned by a given user
        :user: userprofile.UserProfile
        :segment_type: int
        """
        templates = ParamsTemplate.objects.filter(
            owner=user, segment_type=segment_type
        ).order_by("title")
        templates = RelevantPrimaryCategoriesMixin._update_params_missing_perm(user, templates)
        serializer = ParamsTemplateSerializer(templates, many=True)
        return serializer.data


class RelevantPrimaryCategoriesMixin:
    """
    Mixin to check relevant_primary_categories permission,
    validate field
    """

    @staticmethod
    def check_relevant_primary_categories_perm(user, params):
        """
        :param user: userprofile.models.UserProfile
        :param params: dict
        """
        if params.get("relevant_primary_categories", None) is True:
            if not user.has_permission(StaticPermissions.BUILD__CTL_RELEVANT_PRIMARY_CATEGORIES):
                raise PermissionDenied("Missing permission for relevant primary categories.")

    @staticmethod
    def _update_params_missing_perm(user, templates):
        """
        Removes relevant_primary_category=True field in param templates if
        user does not have permission for feature

        :param user: userprofile.models.UserProfile
        :param tempaltes: segment.models.ParamsTemplate
        """
        for template in templates:
            if template.params.get("relevant_primary_categories", None) is True:
                if not user.has_permission(StaticPermissions.BUILD__CTL_RELEVANT_PRIMARY_CATEGORIES):
                    template.params.pop("relevant_primary_categories")
                    template.save()
        return templates
