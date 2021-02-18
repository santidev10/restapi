from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError
from rest_framework.exceptions import PermissionDenied

from userprofile.api.serializers import PermissionItemSerializer
from userprofile.models import PermissionItem
from userprofile.models import Role
from userprofile.models import UserRole
from userprofile.models import UserProfile
from userprofile.constants import StaticPermissions
from utils.views import get_object


class UserPermissionsManagement(APIView):
    """
    Get User Options & Permissions
    """
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.USER_MANAGEMENT),
    )

    def get(self, request):
        user, _ = self._validate_request(request)
        all_perms = PermissionItem.all_perms(as_obj=True)
        enabled_perms = set()
        for p in all_perms:
            enabled = user.perms.get(p.permission)
            enabled = enabled if enabled is not None else p.default_value
            if enabled is True:
                enabled_perms.add(p.permission)
        permissions = PermissionItemSerializer(all_perms, many=True, context=dict(enabled_permissions=enabled_perms)).data
        try:
            role_id = user.user_role.role_id
        except (UserRole.DoesNotExist, Role.DoesNotExist):
            role_id = None
        response_data = {
            "email": user.email,
            "permissions": permissions,
            "role_id": role_id,
        }
        return Response(response_data)

    def post(self, request):
        """
        Update profile
        """
        user, role = self._validate_request(request, updating=True)
        data = self.request.data
        all_permissions = {
            p.permission: p.default_value
            for p in PermissionItem.objects.all()
        }
        valid_perm_values = {True, False}
        errors = []
        for perm_name, value in data.get("permissions", {}).items():
            if perm_name not in all_permissions:
                errors.append(f"Invalid permission name: {perm_name}.")
                continue
            if value not in valid_perm_values:
                errors.append(f"Permission: '{perm_name}' value must be true or false.")
        if errors:
            raise ValidationError(errors)
        user.perms.update(data)
        user.save()

        user_role, _ = UserRole.objects.update_or_create(user=user, defaults=(dict(role=role)))
        return Response({"status": "success"})

    def _validate_request(self, request, updating=False):
        target_user_id = request.query_params.get("user_id")
        if not target_user_id:
            raise ValidationError("Must provide a user_id to manage permissions for.")
        try:
            target_user = UserProfile.objects.get(id=target_user_id)
        except UserProfile.DoesNotExist:
            raise ValidationError("Invalid user id")

        role = None
        # Validate that only admin users can change admin permissions
        if updating is True:
            if target_user.perms.get("admin", False) != request.data.get("admin", False) \
                    and not request.user.has_permission(StaticPermissions.ADMIN):
                raise PermissionDenied("You must be an admin to manage admin permissions.")
            if request.data.get("role_id") is not None:
                role = get_object(Role, id=request.data["role_id"])
        return target_user, role
