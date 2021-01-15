from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError

from userprofile.models import PermissionItem
from userprofile.models import UserProfile
from userprofile.constants import StaticPermissions
from utils.permissions import check_static_permission


class UserPermissionsManagement(APIView):
    """
    Get User Options & Permissions
    """
    permission_classes = (
        check_static_permission(StaticPermissions.USER_MANAGEMENT),
    )

    def get(self, request):
        user = self._validate_request(request)
        response_data = []
        for p in PermissionItem.objects.all():
            enabled = user.perms.get(p.permission)
            if enabled is None:
                enabled = p.default_value
            response_data.append({
                "perm": p.permission,
                "enabled": enabled,
                "text": p.display
            })
        return Response(response_data)

    def post(self, request):
        """
        Update profile
        """
        user = self._validate_request(request)
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

    def _validate_request(self, request):
        user_id = request.query_params.get("user_id")
        if not user_id:
            raise ValidationError("Must provide a user_id to manage permissions for.")
        try:
            user = UserProfile.objects.get(id=user_id)
        except UserProfile.DoesNotExist:
            raise ValidationError("Invalid user id")
        return user
