from rest_framework.generics import DestroyAPIView

from administration.models import UserAction
from userprofile.constants import StaticPermissions


class UserActionDeleteAdminApiView(DestroyAPIView):
    """
    User action delete endpoint
    """
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.USER_MANAGEMENT),)
    queryset = UserAction.objects.all()
