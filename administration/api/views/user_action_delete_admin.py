from rest_framework.generics import DestroyAPIView
from rest_framework.permissions import IsAdminUser

from administration.models import UserAction


class UserActionDeleteAdminApiView(DestroyAPIView):
    """
    User action delete endpoint
    """
    permission_classes = (IsAdminUser,)
    queryset = UserAction.objects.all()