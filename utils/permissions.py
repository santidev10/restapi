from rest_framework import permissions
from rest_framework.authtoken.models import Token


class MediaBuyingAddOnPermission(permissions.IsAuthenticated):
    def has_permission(self, request, view):
        is_authenticated = super(MediaBuyingAddOnPermission,
                                 self).has_permission(request, view)
        return is_authenticated \
               and (request.user.is_staff
                    or request.user.has_perm("userprofile.view_media_buying"))


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


class UserHasPermissionBase(permissions.IsAuthenticated):
    """
    Allow to perform action if user has given django permission
    """
    permission = None

    def has_permission(self, request, view):
        return request.user.has_perm(self.permission)


class UserHasCHFPermission(permissions.IsAuthenticated):
    """
    Allow user to use CHF dashboard data
    """
    def has_permission(self, request, view):
        is_chf = None
        if request.method == "GET":
            is_chf = request.queryparams.get("is_chf")
        if request.method == "POST":
            is_chf = request.data.get("is_chf")
        if str(is_chf) == "1":
            return request.user.is_staff or request.user.has_perm(
                "userprofile.view_dashboard")
        return True


def user_has_permission(perm):
    """
    Create class inherited from UserHasPermissionBase
    with particular permission
    """
    return type("UserHasPermission", (UserHasPermissionBase,),
                dict(permission=perm))


def user_has_any_permission(*perms):
    """
    Create permission class which allows action
    if user has any of listed permission
    """
    classes = map(lambda p: user_has_permission(p), perms)
    return or_permission_classes(*classes)
