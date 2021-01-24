from rest_framework import permissions

from userprofile.constants import StaticPermissions


class PerformIQPermission(permissions.BasePermission):
    def has_permission(self, request, view):
        return request.user.has_permission(StaticPermissions.PERFORMIQ)
