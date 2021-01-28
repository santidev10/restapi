from django.contrib.auth import get_user_model
from rest_framework import permissions

from userprofile.models import UserDeviceToken
from userprofile.constants import StaticPermissions


class MediaBuyingAddOnPermission(permissions.IsAuthenticated):
    def has_permission(self, request, view):
        is_authenticated = super(MediaBuyingAddOnPermission,
                                 self).has_permission(request, view)
        return is_authenticated and request.user.has_permission(StaticPermissions.MEDIA_BUYING)


class OnlyAdminUserCanCreateUpdateDelete(permissions.BasePermission):
    def has_permission(self, request, view):
        if request.method in ["POST", "PUT", "UPDATE", "DELETE"]:
            return request.user and request.user.has_permission(StaticPermissions.ADMIN)
        return True


class OnlyAdminUserOrSubscriber(permissions.BasePermission):
    """
    Allow to perform action only for admin or subscribed user
    """

    def has_permission(self, request, view):
        """
        Check permission
        """
        return request.user.is_authenticated and request.user.has_permission(StaticPermissions.ADMIN)


class IsAuthQueryTokenPermission(permissions.BasePermission):

    def has_permission(self, request, view):
        token = request.query_params.get("auth_token")
        if not token:
            return False
        try:
            UserDeviceToken.objects.get(key=token)
        except UserDeviceToken.DoesNotExist:
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
        return request.user.has_permission(self.permission)


def user_has_permission(perm):
    """
    Create class inherited from UserHasPermissionBase
    with particular permission
    """
    return type("UserHasPermission", (UserHasPermissionBase,),
                dict(permission=perm))


class ExportDataAllowed(permissions.BasePermission):

    def has_permission(self, request, view):
        return request.method == "GET"


class BrandSafetyDataVisible(permissions.BasePermission):

    def has_permission(self, request, *_):
        return request.user and request.user.has_permission(StaticPermissions.RESEARCH__BRAND_SUITABILITY)


class ReadOnly(permissions.BasePermission):
    def has_permission(self, request, view):
        return request.method in view.READ_ONLY


class IsVettingAdmin(permissions.BasePermission):
    def has_permission(self, request, *_):
        return request.user and request.user.has_permission(StaticPermissions.CTL__VET_ADMIN)
