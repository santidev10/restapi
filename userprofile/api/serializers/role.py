from django.db import transaction
from django.contrib.auth import get_user_model
from rest_framework import serializers

from administration.api.serializers import UserSerializer
from userprofile.api.serializers import PermissionItemSerializer
from userprofile.models import PermissionItem
from userprofile.models import Role
from userprofile.models import UserRole


class RoleSerializer(serializers.ModelSerializer):
    permissions = serializers.JSONField()
    users = serializers.ListField(write_only=True, required=False)

    class Meta:
        model = Role
        fields = "__all__"

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data["permissions"] = self._get_permissions(instance)
        data["users"] = self._get_users(instance)
        return data

    def _get_permissions(self, obj):
        """
        Serialize related permissions
        """
        role_permissions = self._update_default(obj.permissions)
        enabled = set(perm for perm in role_permissions if role_permissions[perm] is True)
        all_permissions = PermissionItem.all_perms(as_obj=True)
        serializer = PermissionItemSerializer(all_permissions, many=True, context=dict(enabled_permissions=enabled))
        permissions = serializer.data
        return permissions

    def _get_users(self, instance):
        """
        Serialize users in Role
        """
        users = get_user_model().objects.filter(user_role__role=instance)
        serializer = UserSerializer(users, many=True)
        data = serializer.data
        return data

    def validate_permissions(self, permissions):
        valid_perms = PermissionItem.all_perms()
        invalid_perms = set(permissions.keys()) - set(valid_perms)
        if invalid_perms:
            raise serializers.ValidationError(f"Trying to save invalid permission names: {invalid_perms}")
        return permissions

    def create(self, validated_data):
        permissions = self._update_default(validated_data["permissions"])
        with transaction.atomic():
            role = Role.objects.create(name=validated_data["name"], permissions=permissions)
            self._update_or_create_user_roles(role, validated_data.get("users", []))
        return role

    def update(self, instance, validated_data):
        with transaction.atomic():
            instance.name = validated_data["name"]
            permissions = self._update_default(validated_data["permissions"])
            instance.permissions = permissions
            instance.save(update_fields=["permissions", "name"])
            self._update_or_create_user_roles(instance, validated_data.get("users", []))
        return instance

    def _update_or_create_user_roles(self, role, user_ids):
        user_roles_exist = UserRole.objects.filter(user_id__in=user_ids)
        user_id_roles_to_create = set(user_ids) - set(user_roles_exist.values_list("user_id", flat=True))
        UserRole.objects.bulk_create([
            UserRole(user_id=user_id, role=role) for user_id in user_id_roles_to_create
        ])
        # Update existing user roles to be part of this role
        UserRole.objects.filter(user_id__in=user_ids).update(role=role)
        # Remove existing user roles from this role
        UserRole.objects.filter(role=role).exclude(user_id__in=user_ids).update(role=None)

    def _update_default(self, permissions):
        all_perms = {
            perm: default_value for perm, default_value, _ in PermissionItem.STATIC_PERMISSIONS
        }
        all_perms.update(permissions)
        return all_perms
