from rest_framework.generics import DestroyAPIView

from administration.models import UserAction
from userprofile.constants import StaticPermissions


class UserActionDeleteAdminApiView(DestroyAPIView):
    """
    User action delete endpoint
    """
    permission_classes = (StaticPermissions()(StaticPermissions.ADMIN),)
    queryset = UserAction.objects.all()
