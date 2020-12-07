from rest_framework import permissions


class PerformIQPermission(permissions.BasePermission):
    def has_permission(self, request, view):
        return request.user.has_perm("performiq")
