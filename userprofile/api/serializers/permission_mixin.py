from userprofile.models import PermissionItem
from userprofile.models import Role
from userprofile.models import UserRole


class PermissionSerializerMixin:
    """ Mixin to apply to serializers for UserProfile """
    def get_perms(self, user):
        """ Get permissions and update with default values if missing """
        try:
            role = user.user_role.role
            perms = {
                perm: True
                for perm in role.permissions.all().values_list("permission", flat=True)
            }
        except (AttributeError, UserRole.DoesNotExist, Role.DoesNotExist):
            perms = user.perms
        perms.update({
            perm: default
            for perm, default, _ in PermissionItem.STATIC_PERMISSIONS
            if perm not in perms
        })
        return perms
