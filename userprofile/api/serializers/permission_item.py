from rest_framework import serializers

from userprofile.models import PermissionItem


class PermissionItemSerializer(serializers.ModelSerializer):
    class Meta:
        model = PermissionItem
        fields = "__all__"

    def to_representation(self, instance):
        result = super().to_representation(instance)
        enabled_permissions = self.context.get("enabled_permissions", set())
        result["enabled"] = result["permission"] in enabled_permissions
        return result
