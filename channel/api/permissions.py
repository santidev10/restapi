from rest_framework import permissions


class OnlyAdminUserCanUpdate(permissions.BasePermission):
    def has_permission(self, request, view):
        if request.method in ['PUT', 'UPDATE']:
            return request.user.is_staff
        return True
