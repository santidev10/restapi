from django.db import transaction

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_200_OK
from rest_framework import serializers


from userprofile.api.serializers import RoleSerializer
from userprofile.constants import StaticPermissions
from userprofile.models import PermissionItem
from userprofile.models import UserRole
from userprofile.models import Role
from utils.views import get_object


class UserRoleListCreateAPIView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.USER_MANAGEMENT),
    )

    def get(self, request, *args, **kwargs):
        data = [{
            "id": role.id,
            "name": role.name,
        } for role in Role.objects.all().order_by("id")]
        return Response(data)

    def post(self, request, *args, **kwargs):
        serializer = RoleSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(data=serializer.validated_data)
