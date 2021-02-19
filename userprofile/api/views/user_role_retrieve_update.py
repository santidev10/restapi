from rest_framework.views import APIView
from rest_framework.response import Response

from userprofile.api.serializers import PermissionItemSerializer
from userprofile.api.serializers import RoleSerializer
from userprofile.constants import StaticPermissions
from userprofile.models import PermissionItem
from userprofile.models import Role
from utils.views import get_object


class UserRoleRetrieveUpdateAPIView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.USER_MANAGEMENT),
    )

    def get(self, request, *args, **kwargs):
        # If pk is 0, client just needs list of existing permissions to display
        if str(kwargs.get("pk")) == "0":
            permissions = PermissionItem.all_perms(as_obj=True)
            data = PermissionItemSerializer(permissions, many=True).data
            return Response(data)

        role = get_object(Role, id=kwargs.get("pk"))
        serializer = RoleSerializer(role)
        data = serializer.data
        return Response(data)

    def patch(self, request, *args, **kwargs):
        role = get_object(Role, id=kwargs.get("pk"))
        data = request.data
        serializer = RoleSerializer(role, data=data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        data = serializer.validated_data
        return Response(data)
