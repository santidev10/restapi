from django.contrib.auth import get_user_model
from rest_framework import serializers

from userprofile.api.serializers import PermissionItemSerializer
from userprofile.models import PermissionItem
from userprofile.models import Role


class RoleSerializer(serializers.ModelSerializer):
    permissions = serializers.JSONField()
    users = serializers.SerializerMethodField()

    class Meta:
        model = Role
        fields = "__all__"

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data["permissions"] = self._get_permissions(instance)
        return data

    def _get_permissions(self, obj):
        current_permissions = PermissionItem.all_perms(as_obj=True)
        role_permissions = set(p.permission for p in obj.permissions.all())
        all_permissions = {
            perm["id"]: perm
            for perm in PermissionItemSerializer(current_permissions,
                                                 many=True, context=dict(enabled_permissions=role_permissions)).data
        }
        values = list(all_permissions.values())
        return values

    def validate_permissions(self, permissions):
        valid_perms = PermissionItem.all_perms()
        invalid_perms = set(permissions.keys()) - set(valid_perms)
        if invalid_perms:
            raise serializers.ValidationError(f"Trying to save invalid permission names: {invalid_perms}")
        enabled_permission_names = [p for p in permissions if permissions[p] is True]
        return enabled_permission_names

    def create(self, validated_data):
        permissions = PermissionItem.objects.filter(permission__in=validated_data["permissions"])
        role = Role.objects.create(name=validated_data["name"])
        role.permissions.add(*permissions)
        return role

    def update(self, instance, validated_data):
        instance.name = validated_data["name"]
        instance.save()

        enabled_permission_names = validated_data["permissions"]
        instance.permissions.add(*PermissionItem.objects.filter(permission__in=enabled_permission_names))
        instance.permissions.remove(*PermissionItem.objects.exclude(permission__in=enabled_permission_names))
        return instance

    def get_users(self, instance):
        users = get_user_model().objects.filter(user_role__role=instance)
        users = [{
            "id": user.id,
            "first_name": user.first_name,
            "last_name": user.last_name,
        } for user in users]
        return users
