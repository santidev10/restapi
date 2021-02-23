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
        all_permissions = PermissionItem.all_perms(as_obj=True)
        role_permissions = set(p.permission for p in obj.permissions.all())
        serializer = PermissionItemSerializer(all_permissions, many=True,
                                              context=dict(enabled_permissions=role_permissions))
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
        enabled_permission_names = [p for p in permissions if permissions[p] is True]
        return enabled_permission_names

    def create(self, validated_data):
        with transaction.atomic():
            permissions = PermissionItem.objects.filter(permission__in=validated_data["permissions"])
            role = Role.objects.create(name=validated_data["name"])
            role.permissions.add(*permissions)
            self._update_or_create_user_roles(role, validated_data.get("users", []))
        return role

    def update(self, instance, validated_data):
        with transaction.atomic():
            instance.name = validated_data["name"]
            instance.save()

            enabled_permission_names = validated_data["permissions"]
            instance.permissions.add(*PermissionItem.objects.filter(permission__in=enabled_permission_names))
            instance.permissions.remove(*PermissionItem.objects.exclude(permission__in=enabled_permission_names))
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
