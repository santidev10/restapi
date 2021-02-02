from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError
from rest_framework.exceptions import PermissionDenied

from userprofile.models import PermissionItem
from userprofile.models import UserProfile
from userprofile.constants import StaticPermissions


class UserPermissionsManagement(APIView):
    """
    Get User Options & Permissions
    """
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.USER_MANAGEMENT),
    )

    def get(self, request):
        user = self._validate_request(request)
        permissions = []
        all_valid_perms = set(PermissionItem.all_perms())
        for p in PermissionItem.objects.all():
            if p.permission not in all_valid_perms:
                continue
            enabled = user.perms.get(p.permission)
            if enabled is None:
                enabled = p.default_value
            permissions.append({
                "perm": p.permission,
                "enabled": enabled,
                "text": p.display
            })
        response_data = {
            "email": user.email,
            "permissions": permissions,
        }
        return Response(response_data)

    def post(self, request):
        """
        Update profile
        """
        user = self._validate_request(request, updating=True)
        data = self.request.data
        all_permissions = {
            p.permission: p.default_value
            for p in PermissionItem.objects.all()
        }
        valid_perm_values = {True, False}
        errors = []
        for perm_name, value in data.items():
            if perm_name not in all_permissions:
                errors.append(f"Invalid permission name: {perm_name}.")
                continue
            if value not in valid_perm_values:
                errors.append(f"Permission: '{perm_name}' value must be true or false.")
        if errors:
            raise ValidationError(errors)
        user.perms.update(data)
        user.save()
        return Response({"status": "success"})

    def _validate_request(self, request, updating=False):
        target_user_id = request.query_params.get("user_id")
        if not target_user_id:
            raise ValidationError("Must provide a user_id to manage permissions for.")
        try:
            target_user = UserProfile.objects.get(id=target_user_id)
        except UserProfile.DoesNotExist:
            raise ValidationError("Invalid user id")

        # Validate that only admin users can change admin permissions
        if updating is True and target_user.perms.get("admin", False) != request.data.get("admin", False) \
                and not request.user.has_permission(StaticPermissions.ADMIN):
            raise PermissionDenied("You must be an admin to manage admin permissions.")

        return target_user
