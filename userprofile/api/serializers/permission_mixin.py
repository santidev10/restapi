from userprofile.models import PermissionItem
from userprofile.models import Role
from userprofile.models import UserRole


class PermissionSerializerMixin:
    """ Mixin to apply to serializers for UserProfile """
    def get_perms(self, user):
        """ Get permissions and update with default values if missing """
        all_perms = {
            perm: default for perm, default, _ in PermissionItem.STATIC_PERMISSIONS
        }
        try:
            role = user.user_role.role
            perms = role.permissions
        except (AttributeError, UserRole.DoesNotExist, Role.DoesNotExist):
            perms = user.perms
        all_perms.update(perms)
        return all_perms
