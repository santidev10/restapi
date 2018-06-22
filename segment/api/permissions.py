from rest_framework import permissions


class SegmentOwnerPermission(permissions.IsAuthenticated):
    def has_permission(self, request, view):
        owner_id = request.query_params.get("owner_id")
        if owner_id is not None:
            return request.user.is_authenticated() and (
                    (str(request.user.id) == owner_id)
                    or request.user.is_staff)
        return super(SegmentOwnerPermission, self).has_permission(
            request, view)
