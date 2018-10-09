from django.contrib.auth.models import Group
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from userprofile.api.serializers import GroupSerializer


class AccessGroupsListApiView(ListAPIView):
    """
    User permissions groups endpoing
    """
    permission_classes = (IsAdminUser,)
    serializer_class = GroupSerializer
    queryset = Group.objects.all()