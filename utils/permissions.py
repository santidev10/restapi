from rest_framework import permissions
from rest_framework.authtoken.models import Token


class MediaBuyingAddOnPermission(permissions.IsAuthenticated):
    def has_permission(self, request, view):
        is_authenticated = super(MediaBuyingAddOnPermission,
                                 self).has_permission(request, view)
        return is_authenticated and (
                request.user.is_staff or request.user.can_access_media_buying)


class OnlyAdminUserCanCreateUpdateDelete(permissions.BasePermission):
    def has_permission(self, request, view):
        if request.method in ['POST', 'PUT', 'UPDATE', 'DELETE']:
            return request.user.is_staff
        return True


class OnlyAdminUserOrSubscriber(permissions.BasePermission):
    """
    Allow to perform action only for admin or subscribed user
    """

    def has_permission(self, request, view):
        """
        Check permission
        """
        return request.user.is_authenticated() and request.user.is_staff


class IsAuthQueryTokenPermission(permissions.BasePermission):

    def has_permission(self, request, view):
        token = request.query_params.get("auth_token")
        if not token:
            return False
        try:
            Token.objects.get(key=token)
        except Token.DoesNotExist:
            return False
        return True


class OrPermissionsBase(permissions.BasePermission):
    """
    Allow to perform action if any class from `classes` allows it
    """
    classes = []

    def has_permission(self, request, view):
        for cls in self.classes:
            if cls().has_permission(request, view):
                return True
        return False


def or_permission_classes(*classes):
    """
    Create new class inherited from `OrPermissionsBase`
    with particular classes list
    """
    return type("OrPermissions", (OrPermissionsBase,), dict(classes=classes))
