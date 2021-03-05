import time
from datetime import datetime

from django.contrib.auth import get_user_model
from rest_framework import permissions
from rest_framework.exceptions import ValidationError

from audit_tool.models import AuditProcessor
from audit_tool.constants import CHOICE_UNKNOWN_KEY
from audit_tool.constants import CHOICE_UNKNOWN_NAME
import brand_safety.constants as constants
from segment.models import CustomSegment
from segment.models.persistent.base import BasePersistentSegment
from userprofile.constants import StaticPermissions


class ModelDoesNotExist(Exception):
    pass


@property
def SEGMENT_TYPES():
    return [constants.CHANNEL, constants.VIDEO]


@property
def PERSISTENT_SEGMENT_MODELS():
    return list(BasePersistentSegment.__subclasses__())


@property
def PERSISTENT_SEGMENT_TYPES():
    return [m.segment_type for m in PERSISTENT_SEGMENT_MODELS.fget()]


def get_persistent_segment_model_by_type(segment_type):
    for model in PERSISTENT_SEGMENT_MODELS.fget():
        if model.segment_type == segment_type:
            return model
    raise ModelDoesNotExist("Invalid segment_type: %s" % segment_type)


def retry_on_conflict(method, *args, retry_amount=5, sleep_coeff=2, **kwargs):
    """
    Retry on Document Conflicts
    """
    tries_count = 0
    while tries_count <= retry_amount:
        try:
            result = method(*args, **kwargs)
        # pylint: disable=broad-except
        except Exception as err:
            # pylint: enable=broad-except
            if "ConflictError(409" in str(err):
                tries_count += 1
                if tries_count <= retry_amount:
                    sleep_seconds_count = tries_count ** sleep_coeff
                    time.sleep(sleep_seconds_count)
            else:
                raise err
        else:
            return result


def generate_search_with_params(manager, query, sort=None):
    """
    Generate scan query with sorting
    :param manager:
    :param query:
    :param sort:
    :return:
    """
    # pylint: disable=protected-access
    search = manager._search()
    # pylint: enable=protected-access
    search = search.query(query)
    if sort:
        search = search.sort(sort)
    search = search.params(preserve_order=True)
    return search


def validate_threshold(threshold):
    err = None
    if not 0 <= threshold <= 100:
        err = "Score threshold must be between 0 and 100, inclusive."
    return err


def validate_date(date_str: str):
    if date_str:
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date: {date_str}. Date format must be YYYY-MM-DD")
    return date_str


def validate_numeric(value):
    formatted = str(value).replace(",", "")
    try:
        to_num = int(formatted)
    except ValueError:
        raise ValueError(f"The value: '{value}' is not a valid number.")
    return to_num


def validate_boolean(value):
    if isinstance(value, bool) or (isinstance(value, int) and value in [0, 1]):
        return bool(value)
    raise ValueError(f"The value: '{value}' is not a valid boolean.")


def validate_in(member, container: list):
    if member not in container:
        valid_values = ", ".join(map(str, container))
        raise ValueError(f"The value: '{member}' must be one of the following: {valid_values}")
    return member


def validate_all_in(members: list, container: list) -> list:
    if not isinstance(members, list):
        raise ValueError(f"'{str(members)}' should be a list.")
    return [validate_in(member, container) for member in members]


def with_unknown(options=None, choice=None):
    """
    If choice is None, create dict mapping of id, name for list of two element tuple options
    Adds an id of -1 that will map to "All"

    If choice is not None, then implies that we should map choice to either:
     if choice == -1:
        list as we want to include everything
     else:
        list of single of multiple element list for Elasticseach terms query
    :param options: list [tuple(int, str)...] List of two element tuple choices
    :param choice: None | int | list Input that will be mapped into list of terms for Elasticsearch query
    :return:
    """
    if options is None and choice is None:
        return None

    if options:
        data = [{"id": _id, "name": name} for _id, name in options]
        data.append({
            "id": CHOICE_UNKNOWN_KEY,
            "name": CHOICE_UNKNOWN_NAME,
        })
        return data

    if isinstance(choice, list):
        return choice

    choice = int(choice)
    if choice == -1:
        return []

    return choice


def get_content_disposition(segment, is_vetting=False, ext="csv"):
    title = segment.title
    if is_vetting is True:
        title += " Vetted"
    content_disposition = 'attachment;filename="{title}.{ext}"'.format(title=title, ext=ext)
    return content_disposition


def set_user_perm_params(request, ctl_params):
    """
    Modify params using for CTL depending on user permissions
    :param request: View request object
    :param ctl_params: dict: Request body data used for CTL
    :return:
    """
    # If user is not admin / vetting admin, force create vetted safe only
    if not (request.user and request.user.has_permission(StaticPermissions.BUILD__CTL_EXPORT_ADMIN)) \
            or not request.user.has_permission(StaticPermissions.BUILD__CTL_VET_ADMIN):
        ctl_params["vetting_status"] = [1]
    return ctl_params


def delete_related(segment, *_, **__):
    """ Delete CTL and related objects in case of exceptions while creating ctl """

    def _delete_audit(audit_id):
        try:
            AuditProcessor.objects.get(id=audit_id).delete()
        except AuditProcessor.DoesNotExist:
            pass

    if isinstance(segment, str):
        try:
            segment = CustomSegment.objects.get(id=segment)
        except CustomSegment.DoesNotExist:
            return
    _delete_audit(segment.audit_id)
    _delete_audit(segment.params.get("meta_audit_id"))
    segment.delete()


class AdminCustomSegmentOwnerPermission(permissions.BasePermission):
    """ Check if user is admin or is CTL creator """
    def has_permission(self, request, view):
        if isinstance(request.user, get_user_model()) and request.user.has_permission(StaticPermissions.BUILD__CTL_EXPORT_ADMIN):
            return True
        try:
            segment = CustomSegment.objects.get(id=view.kwargs["pk"])
        except CustomSegment.DoesNotExist:
            raise ValidationError(f"Custom Segment with id {view.kwargs['pk']} does not exist.")
        return request.user.has_permission(StaticPermissions.BUILD__CTL_EXPORT_ADMIN) or segment.owner == request.user
