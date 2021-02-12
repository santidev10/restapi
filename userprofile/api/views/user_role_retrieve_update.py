from rest_framework.views import APIView
from rest_framework.response import Response

from userprofile.api.serializers import RoleSerializer
from userprofile.constants import StaticPermissions
from userprofile.models import Role
from utils.views import get_object


class UserRoleRetrieveUpdateAPIView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.USER_MANAGEMENT),
    )

    def get(self, request, *args, **kwargs):
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
