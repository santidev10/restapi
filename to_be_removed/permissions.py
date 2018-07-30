from rest_framework import permissions


def is_chf_in_request_deprecated(request):
    is_chf = None
    if request.method == "GET":
        is_chf = request.query_params.get("is_chf")
    if request.method == "POST":
        is_chf = request.data.get("is_chf")
    return str(is_chf) == "1"


class UserHasDashboardPermissionDeprecated(permissions.IsAuthenticated):
    """
    Allow user to use CHF dashboard data
    """

    def has_permission(self, request, view):
        if is_chf_in_request_deprecated(request):
            return request.user.has_perm("userprofile.view_dashboard")
        return True


class UserHasDashboardOrStaffPermissionDeprecated(UserHasDashboardPermissionDeprecated):
    """
    Allow user to use CHF dashboard data
    """

    def has_permission(self, request, view):
        if is_chf_in_request_deprecated(request):
            return request.user.is_staff \
                   or super(UserHasDashboardOrStaffPermissionDeprecated, self) \
                       .has_permission(request, view)
        return True
